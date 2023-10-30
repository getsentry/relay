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

#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Array, Empty, FromValue, IntoValue};

use crate::processor::ProcessValue;
use crate::protocol::{
    ClientSdkInfo, Contexts, EventId, LenientString, Request, Tags, Timestamp, User,
};

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
    #[metastructure(pii = "false")]
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
    #[metastructure(max_chars = "environment")]
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
    #[metastructure(pii = "true", bag_size = "large")]
    pub urls: Annotated<Array<String>>,

    /// A list of error-ids discovered during the lifetime of the segment.
    #[metastructure(bag_size = "medium")]
    pub error_ids: Annotated<Array<String>>,

    /// A list of trace-ids discovered during the lifetime of the segment.
    #[metastructure(bag_size = "medium")]
    pub trace_ids: Annotated<Array<String>>,

    /// Contexts describing the environment (e.g. device, os or browser).
    #[metastructure(skip_serialization = "empty")]
    pub contexts: Annotated<Contexts>,

    /// Platform identifier of this event (defaults to "other").
    ///
    /// A string representing the platform the SDK is submitting from. This will be used by the
    /// Sentry interface to customize various components in the interface.
    #[metastructure(max_chars = "environment")]
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
        nonempty = "true",
        max_chars = "environment"
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
    #[metastructure(field = "type", max_chars = "environment")]
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

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use crate::protocol::TagEntry;

    use super::*;

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
}
