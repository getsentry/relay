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

use relay_protocol::{Annotated, Array, Empty, FromValue, Getter, IntoValue, Val};

use crate::processor::ProcessValue;
use crate::protocol::{
    AppContext, BrowserContext, ClientSdkInfo, Contexts, DefaultContext, DeviceContext, EventId,
    LenientString, OsContext, ProfileContext, Request, ResponseContext, Tags, Timestamp,
    TraceContext, User,
};
use uuid::Uuid;

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
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
    #[metastructure(max_chars = 64)]
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
    #[metastructure(pii = "true", max_depth = 7, max_bytes = 8192)]
    pub urls: Annotated<Array<String>>,

    /// A list of error-ids discovered during the lifetime of the segment.
    #[metastructure(max_depth = 5, max_bytes = 2048)]
    pub error_ids: Annotated<Array<Uuid>>,

    /// A list of trace-ids discovered during the lifetime of the segment.
    #[metastructure(max_depth = 5, max_bytes = 2048)]
    pub trace_ids: Annotated<Array<Uuid>>,

    /// Contexts describing the environment (e.g. device, os or browser).
    #[metastructure(skip_serialization = "empty")]
    pub contexts: Annotated<Contexts>,

    /// Platform identifier of this event (defaults to "other").
    ///
    /// A string representing the platform the SDK is submitting from. This will be used by the
    /// Sentry interface to customize various components in the interface.
    #[metastructure(max_chars = 64)]
    pub platform: Annotated<String>,

    /// The release version of the application.
    ///
    /// **Release versions must be unique across all projects in your organization.** This value
    /// can be the git SHA for the given project, or a product identifier with a semantic version.
    #[metastructure(
        max_chars = 200,
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
        max_chars = 64
    )]
    pub dist: Annotated<String>,

    /// The environment name, such as `production` or `staging`.
    ///
    /// ```json
    /// { "environment": "production" }
    /// ```
    #[metastructure(
        max_chars = 64,
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
    #[metastructure(field = "type", max_chars = 64)]
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
    /// Returns a reference to the context if it exists in its default key.
    pub fn context<C: DefaultContext>(&self) -> Option<&C> {
        self.contexts.value()?.get()
    }

    /// Returns the raw user agent string.
    ///
    /// Returns `Some` if the event's request interface contains a `user-agent` header. Returns
    /// `None` otherwise.
    pub fn user_agent(&self) -> Option<&str> {
        let headers = self.request.value()?.headers.value()?;

        for item in headers.iter() {
            if let Some((ref o_k, ref v)) = item.value() {
                if let Some(k) = o_k.as_str() {
                    if k.to_lowercase() == "user-agent" {
                        return v.as_str();
                    }
                }
            }
        }

        None
    }
}

impl Getter for Replay {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        Some(match path.strip_prefix("event.")? {
            // Simple fields
            "release" => self.release.as_str()?.into(),
            "dist" => self.dist.as_str()?.into(),
            "environment" => self.environment.as_str()?.into(),
            "platform" => self.platform.as_str().unwrap_or("other").into(),

            // Fields in top level structures (called "interfaces" in Sentry)
            "user.email" => or_none(&self.user.value()?.email)?.into(),
            "user.id" => or_none(&self.user.value()?.id)?.into(),
            "user.ip_address" => self.user.value()?.ip_address.as_str()?.into(),
            "user.name" => self.user.value()?.name.as_str()?.into(),
            "user.segment" => or_none(&self.user.value()?.segment)?.into(),
            "user.geo.city" => self.user.value()?.geo.value()?.city.as_str()?.into(),
            "user.geo.country_code" => self
                .user
                .value()?
                .geo
                .value()?
                .country_code
                .as_str()?
                .into(),
            "user.geo.region" => self.user.value()?.geo.value()?.region.as_str()?.into(),
            "user.geo.subdivision" => self.user.value()?.geo.value()?.subdivision.as_str()?.into(),
            "request.method" => self.request.value()?.method.as_str()?.into(),
            "request.url" => self.request.value()?.url.as_str()?.into(),
            "sdk.name" => self.sdk.value()?.name.as_str()?.into(),
            "sdk.version" => self.sdk.value()?.version.as_str()?.into(),

            // Computed fields (after normalization).
            "sentry_user" => self.user.value()?.sentry_user.as_str()?.into(),

            // Partial implementation of contexts.
            "contexts.app.in_foreground" => {
                self.context::<AppContext>()?.in_foreground.value()?.into()
            }
            "contexts.device.arch" => self.context::<DeviceContext>()?.arch.as_str()?.into(),
            "contexts.device.battery_level" => self
                .context::<DeviceContext>()?
                .battery_level
                .value()?
                .into(),
            "contexts.device.brand" => self.context::<DeviceContext>()?.brand.as_str()?.into(),
            "contexts.device.charging" => self.context::<DeviceContext>()?.charging.value()?.into(),
            "contexts.device.family" => self.context::<DeviceContext>()?.family.as_str()?.into(),
            "contexts.device.model" => self.context::<DeviceContext>()?.model.as_str()?.into(),
            "contexts.device.locale" => self.context::<DeviceContext>()?.locale.as_str()?.into(),
            "contexts.device.online" => self.context::<DeviceContext>()?.online.value()?.into(),
            "contexts.device.orientation" => self
                .context::<DeviceContext>()?
                .orientation
                .as_str()?
                .into(),
            "contexts.device.name" => self.context::<DeviceContext>()?.name.as_str()?.into(),
            "contexts.device.screen_density" => self
                .context::<DeviceContext>()?
                .screen_density
                .value()?
                .into(),
            "contexts.device.screen_dpi" => {
                self.context::<DeviceContext>()?.screen_dpi.value()?.into()
            }
            "contexts.device.screen_width_pixels" => self
                .context::<DeviceContext>()?
                .screen_width_pixels
                .value()?
                .into(),
            "contexts.device.screen_height_pixels" => self
                .context::<DeviceContext>()?
                .screen_height_pixels
                .value()?
                .into(),
            "contexts.device.simulator" => {
                self.context::<DeviceContext>()?.simulator.value()?.into()
            }
            "contexts.os.build" => self.context::<OsContext>()?.build.as_str()?.into(),
            "contexts.os.kernel_version" => {
                self.context::<OsContext>()?.kernel_version.as_str()?.into()
            }
            "contexts.os.name" => self.context::<OsContext>()?.name.as_str()?.into(),
            "contexts.os.version" => self.context::<OsContext>()?.version.as_str()?.into(),
            "contexts.browser.name" => self.context::<BrowserContext>()?.name.as_str()?.into(),
            "contexts.browser.version" => {
                self.context::<BrowserContext>()?.version.as_str()?.into()
            }
            "contexts.profile.profile_id" => self
                .context::<ProfileContext>()?
                .profile_id
                .value()?
                .0
                .into(),
            "contexts.device.uuid" => self.context::<DeviceContext>()?.uuid.value()?.into(),
            "contexts.trace.status" => self
                .context::<TraceContext>()?
                .status
                .value()?
                .as_str()
                .into(),
            "contexts.trace.op" => self.context::<TraceContext>()?.op.as_str()?.into(),
            "contexts.response.status_code" => self
                .context::<ResponseContext>()?
                .status_code
                .value()?
                .into(),
            "contexts.unreal.crash_type" => match self.contexts.value()?.get_key("unreal")? {
                super::Context::Other(context) => context.get("crash_type")?.value()?.into(),
                _ => return None,
            },

            // Dynamic access to certain data bags
            path => {
                if let Some(rest) = path.strip_prefix("tags.") {
                    self.tags.value()?.get(rest)?.into()
                } else if let Some(rest) = path.strip_prefix("request.headers.") {
                    self.request
                        .value()?
                        .headers
                        .value()?
                        .get_header(rest)?
                        .into()
                } else {
                    return None;
                }
            }
        })
    }
}

fn or_none(string: &Annotated<impl AsRef<str>>) -> Option<&str> {
    match string.as_str() {
        None | Some("") => None,
        Some(other) => Some(other),
    }
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
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )]),
            trace_ids: Annotated::new(vec![Annotated::new(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
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
