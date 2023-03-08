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

use std::fmt::Display;
use std::net::IpAddr as RealIPAddr;

use crate::protocol::{
    ClientSdkInfo, Contexts, EventId, IpAddr, LenientString, Request, Tags, Timestamp, User,
};
use crate::store::{self, user_agent};
use crate::types::{Annotated, Array};
use crate::user_agent::RawUserAgentInfo;

#[derive(Debug)]
pub enum ReplayError {
    CouldNotParse(serde_json::Error),
    NoContent,
    InvalidPayload(String),
    CouldNotScrub(String),
}

impl Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::CouldNotParse(e) => write!(f, "{e}"),
            ReplayError::NoContent => write!(f, "No data found.",),
            ReplayError::InvalidPayload(e) => write!(f, "{e}"),
            ReplayError::CouldNotScrub(e) => write!(f, "{e}"),
        }
    }
}

impl From<serde_json::Error> for ReplayError {
    fn from(err: serde_json::Error) -> Self {
        ReplayError::CouldNotParse(err)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_replay", value_type = "Replay")]
pub struct Replay {
    /// Unique identifier of this event.
    ///
    /// Hexadecimal string representing a uuid4 value. The length is exactly 32 characters. Dashes
    /// are not allowed. Has to be lowercase.
    ///
    /// Even though this field is backfilled on the server with a new uuid4, it is strongly
    /// recommended to generate that uuid4 clientside. There are some features like user feedback
    /// which are easier to implement that way, and debugging in case events get lost in your
    /// Sentry installation is also easier.
    ///
    /// Example:
    ///
    /// ```json
    /// {
    ///   "event_id": "fc6d8c0c43fc4630ad850ee518f1b9d0"
    /// }
    /// ```
    pub event_id: Annotated<EventId>,

    /// Replay identifier.
    ///
    /// Hexadecimal string representing a uuid4 value. The length is exactly 32 characters. Dashes
    /// are not allowed. Has to be lowercase.
    ///
    /// Example:
    ///
    /// ```json
    /// {
    ///   "replay_id": "fc6d8c0c43fc4630ad850ee518f1b9d0"
    /// }
    /// ```
    pub replay_id: Annotated<EventId>,

    /// The type of sampling that captured the replay.
    ///
    /// A string enumeration.  One of "session" or "error".
    ///
    /// Example:
    ///
    /// ```json
    /// {
    ///   "replay_type": "session"
    /// }
    /// ```
    pub replay_type: Annotated<String>,

    /// Segment identifier.
    ///
    /// A number representing a unique segment identifier in the chain of replay segments.
    /// Segment identifiers are temporally ordered but can be received by the Relay service in any
    /// order.
    ///
    /// Example:
    ///
    /// ```json
    /// {
    ///   "segment_id": 10
    /// }
    /// ```
    pub segment_id: Annotated<u64>,

    /// Timestamp when the event was created.
    ///
    /// Indicates when the segment was created in the Sentry SDK. The format is either a string as
    /// defined in [RFC 3339](https://tools.ietf.org/html/rfc3339) or a numeric (integer or float)
    /// value representing the number of seconds that have elapsed since the [Unix
    /// epoch](https://en.wikipedia.org/wiki/Unix_time).
    ///
    /// Timezone is assumed to be UTC if missing.
    ///
    /// Sub-microsecond precision is not preserved with numeric values due to precision
    /// limitations with floats (at least in our systems). With that caveat in mind, just send
    /// whatever is easiest to produce.
    ///
    /// All timestamps in the event protocol are formatted this way.
    ///
    /// # Example
    ///
    /// All of these are the same date:
    ///
    /// ```json
    /// { "timestamp": "2011-05-02T17:41:36Z" }
    /// { "timestamp": "2011-05-02T17:41:36" }
    /// { "timestamp": "2011-05-02T17:41:36.000" }
    /// { "timestamp": 1304358096.0 }
    /// ```
    pub timestamp: Annotated<Timestamp>,

    /// Timestamp when the replay was created.  Typically only specified on the initial segment.
    pub replay_start_timestamp: Annotated<Timestamp>,

    /// A list of URLs visted during the lifetime of the segment.
    pub urls: Annotated<Array<String>>,

    /// A list of error-ids discovered during the lifetime of the segment.
    pub error_ids: Annotated<Array<String>>,

    /// A list of trace-ids discovered during the lifetime of the segment.
    pub trace_ids: Annotated<Array<String>>,

    /// Contexts describing the environment (e.g. device, os or browser).
    #[metastructure(skip_serialization = "empty")]
    pub contexts: Annotated<Contexts>,

    /// Platform identifier of this event (defaults to "other").
    ///
    /// A string representing the platform the SDK is submitting from. This will be used by the
    /// Sentry interface to customize various components in the interface.
    pub platform: Annotated<String>,

    /// The release version of the application.
    ///
    /// **Release versions must be unique across all projects in your organization.** This value
    /// can be the git SHA for the given project, or a product identifier with a semantic version.
    #[metastructure(
        max_chars = "tag_value",
        required = "false",
        trim_whitespace = "true",
        nonempty = "true",
        skip_serialization = "empty"
    )]
    pub release: Annotated<LenientString>,

    /// Program's distribution identifier.
    ///
    /// The distribution of the application.
    ///
    /// Distributions are used to disambiguate build or deployment variants of the same release of
    /// an application. For example, the dist can be the build number of an XCode build or the
    /// version code of an Android build.
    #[metastructure(
        allow_chars = "a-zA-Z0-9_.-",
        trim_whitespace = "true",
        required = "false",
        nonempty = "true"
    )]
    pub dist: Annotated<String>,

    /// The environment name, such as `production` or `staging`.
    ///
    /// ```json
    /// { "environment": "production" }
    /// ```
    #[metastructure(
        max_chars = "environment",
        nonempty = "true",
        required = "false",
        trim_whitespace = "true"
    )]
    pub environment: Annotated<String>,

    /// Custom tags for this event.
    ///
    /// A map or list of tags for this event. Each tag must be less than 200 characters.
    #[metastructure(skip_serialization = "empty", pii = "true")]
    pub tags: Annotated<Tags>,

    /// Static value. Should always be "replay_event".
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,

    /// Information about the user who triggered this event.
    #[metastructure(skip_serialization = "empty")]
    pub user: Annotated<User>,

    /// Information about a web request that occurred during the event.
    #[metastructure(skip_serialization = "empty")]
    pub request: Annotated<Request>,

    /// Information about the Sentry SDK that generated this event.
    #[metastructure(field = "sdk")]
    #[metastructure(skip_serialization = "empty")]
    pub sdk: Annotated<ClientSdkInfo>,
}

impl Replay {
    pub fn validate(&mut self) -> Result<(), ReplayError> {
        self.replay_id
            .value()
            .ok_or_else(|| ReplayError::InvalidPayload("missing replay_id".to_string()))?;
        Ok(())
    }

    pub fn normalize(
        &mut self,
        client_ip: Option<RealIPAddr>,
        user_agent: &RawUserAgentInfo<&str>,
    ) {
        self.normalize_platform();
        self.normalize_ip_address(client_ip);
        self.normalize_user_agent(user_agent);
        self.normalize_type();
        self.normalize_array_fields();
    }

    fn normalize_array_fields(&mut self) {
        if let Some(items) = self.error_ids.value_mut() {
            items.truncate(100);
        }

        if let Some(items) = self.trace_ids.value_mut() {
            items.truncate(100);
        }

        if let Some(items) = self.urls.value_mut() {
            items.truncate(100);
        }

    }

    fn normalize_ip_address(&mut self, ip_address: Option<RealIPAddr>) {
        store::normalize_ip_addresses(
            &mut self.request,
            &mut self.user,
            self.platform.as_str(),
            ip_address.map(|ip| IpAddr(ip.to_string())).as_ref(),
        )
    }

    fn normalize_user_agent(&mut self, default_user_agent: &RawUserAgentInfo<&str>) {
        let headers = match self
            .request
            .value()
            .and_then(|request| request.headers.value())
        {
            Some(headers) => headers,
            None => return,
        };

        let user_agent_info = RawUserAgentInfo::from_headers(headers);

        let user_agent_info = if user_agent_info.is_empty() {
            default_user_agent
        } else {
            &user_agent_info
        };

        let contexts = self.contexts.get_or_insert_with(|| Contexts::new());
        user_agent::normalize_user_agent_info_generic(contexts, &self.platform, user_agent_info);
    }

    fn normalize_platform(&mut self) {
        // Null platforms are permitted but must be defaulted before continuing.
        let platform = self.platform.get_or_insert_with(|| "other".to_string());

        // Normalize bad platforms to "other" type.
        if !store::is_valid_platform(platform) {
            self.platform = Annotated::from("other".to_string());
        }
    }

    fn normalize_type(&mut self) {
        self.ty = Annotated::from("replay_event".to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use chrono::{TimeZone, Utc};

    use crate::pii::{DataScrubbingConfig, PiiProcessor};
    use crate::processor::process_value;
    use crate::processor::ProcessingState;
    use crate::protocol::{
        BrowserContext, Context, ContextInner, DeviceContext, EventId, OsContext, Replay, TagEntry,
        Tags,
    };
    use crate::testutils::get_value;
    use crate::types::Annotated;
    use crate::user_agent::RawUserAgentInfo;

    #[test]
    fn test_event_roundtrip() {
        // NOTE: Interfaces will be tested separately.
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "replay_id": "52df9022835246eeb317dbd739ccd059",
  "segment_id": 0,
  "replay_type": "session",
  "error_sample_rate": 0.5,
  "session_sample_rate": 0.5,
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
            event_id: Annotated::new(EventId("52df9022835246eeb317dbd739ccd059".parse().unwrap())),
            replay_id: Annotated::new(EventId("52df9022835246eeb317dbd739ccd059".parse().unwrap())),
            replay_type: Annotated::new("session".to_string()),
            segment_id: Annotated::new(0),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            replay_start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
        replay_value.normalize(None, &RawUserAgentInfo::default());

        let loaded_browser_context = replay_value
            .contexts
            .value()
            .unwrap()
            .get("browser")
            .unwrap();

        let loaded_os_context = replay_value
            .contexts
            .value()
            .unwrap()
            .get("client_os")
            .unwrap();

        let loaded_device_context = replay_value
            .contexts
            .value()
            .unwrap()
            .get("device")
            .unwrap();

        assert_eq!(loaded_browser_context, &browser_context);
        assert_eq!(loaded_os_context, &os_context);
        assert_eq!(loaded_device_context, &device_context);
    }

    #[test]
    fn test_missing_user() {
        let payload = include_str!("../../tests/fixtures/replays/replay_missing_user.json");

        let mut annotated_replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        let replay = annotated_replay.value_mut().as_mut().unwrap();

        // No user object and no ip-address was provided.
        replay.normalize(None, &RawUserAgentInfo::default());
        let user = replay.user.value();
        assert!(user.is_none());

        // No user object but an ip-address was provided.
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        replay.normalize(Some(ip_address), &RawUserAgentInfo::default());

        let ip_addr = replay
            .user
            .value()
            .unwrap()
            .ip_address
            .value()
            .unwrap()
            .as_str();
        assert!(ip_addr == "127.0.0.1");
    }

    #[test]
    fn test_set_ip_address_missing_user_ip_address() {
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // IP-Address set.
        let payload =
            include_str!("../../tests/fixtures/replays/replay_missing_user_ip_address.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        let replay_value = replay.value_mut().as_mut().unwrap();
        replay_value.normalize(Some(ip_address), &RawUserAgentInfo::default());

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
        replay_value.normalize(None, &RawUserAgentInfo::default());

        let user = replay_value.user.value().unwrap();
        assert!(user.ip_address.value().unwrap().as_str() == "127.1.1.1");
        assert!(user.username.value().is_none());
        assert!(user.email.value().unwrap().as_str() == "email@sentry.io");
        assert!(user.id.value().unwrap().as_str() == "1");
    }

    #[test]
    fn test_scrub_pii_from_annotated_replay() {
        let scrub_config = simple_enabled_config();
        let pii_config = scrub_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        let payload = include_str!("../../tests/fixtures/replays/replay.json");
        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        process_value(&mut replay, &mut pii_processor, ProcessingState::root()).unwrap();

        // Ip-address was removed.
        let ip_address = get_value!(replay.user.ip_address);
        assert!(ip_address.is_none());

        let maybe_credit_card = replay
            .value()
            .unwrap()
            .tags
            .value()
            .unwrap()
            .get("credit-card");

        assert_eq!(maybe_credit_card, Some("[Filtered]"));
    }

    #[test]
    fn test_capped_values() {
        let urls: Vec<Annotated<String>> = (0..101)
            .map(|_| Annotated::new("localhost:9000".to_string()))
            .collect();

        let error_ids: Vec<Annotated<String>> = (0..101)
            .map(|_| Annotated::new("52df9022835246eeb317dbd739ccd059".to_string()))
            .collect();

        let trace_ids: Vec<Annotated<String>> = (0..101)
            .map(|_| Annotated::new("52df9022835246eeb317dbd739ccd059".to_string()))
            .collect();

        let mut replay = Annotated::new(Replay {
            urls: Annotated::new(urls),
            error_ids: Annotated::new(error_ids),
            trace_ids: Annotated::new(trace_ids),
            ..Default::default()
        });

        let replay_value = replay.value_mut().as_mut().unwrap();
        replay_value.normalize_array_fields();

        assert!(replay_value.error_ids.value().unwrap().len() == 100);
        assert!(replay_value.trace_ids.value().unwrap().len() == 100);
        assert!(replay_value.urls.value().unwrap().len() == 100);
    }

    fn simple_enabled_config() -> DataScrubbingConfig {
        let mut scrub_config = DataScrubbingConfig::default();
        scrub_config.scrub_data = true;
        scrub_config.scrub_defaults = true;
        scrub_config.scrub_ip_addresses = true;
        scrub_config
    }
}
