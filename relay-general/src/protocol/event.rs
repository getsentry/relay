use std::fmt;
use std::str::FromStr;

#[cfg(feature = "jsonschema")]
use schemars::gen::SchemaGenerator;
#[cfg(feature = "jsonschema")]
use schemars::schema::Schema;

use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{
    Breadcrumb, ClientSdkInfo, Contexts, Csp, DebugMeta, Exception, ExpectCt, ExpectStaple,
    Fingerprint, Hpkp, LenientString, Level, LogEntry, Measurements, Metrics, Request, Span,
    Stacktrace, Tags, TemplateInfo, Thread, Timestamp, User, Values,
};
use crate::types::{
    Annotated, Array, Empty, ErrorKind, FromValue, Object, SkipSerialization, ToValue, Value,
};

/// Wrapper around a UUID with slightly different formatting.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct EventId(pub uuid::Uuid);

impl EventId {
    /// Creates a new event id using a UUID v4.
    #[inline]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Tests if the UUID is nil.
    #[inline]
    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl Default for EventId {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

derive_string_meta_structure!(EventId, "event id");

impl ProcessValue for EventId {}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_simple_ref())
    }
}

impl FromStr for EventId {
    type Err = uuid::Error;

    fn from_str(uuid_str: &str) -> Result<Self, Self::Err> {
        uuid_str.parse().map(EventId)
    }
}

impl_str_serde!(EventId);

#[doc(inline)]
pub use relay_common::{EventType, ParseEventTypeError};

impl Empty for EventType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for EventType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), mut meta) => match value.parse() {
                Ok(eventtype) => Annotated(Some(eventtype), meta),
                Err(_) => {
                    meta.add_error(ErrorKind::InvalidData);
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ToValue for EventType {
    fn to_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(format!("{}", self))
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for EventType {}

#[derive(Debug, FromValue, ToValue, ProcessValue, Empty, Clone, PartialEq)]
pub struct ExtraValue(#[metastructure(bag_size = "larger")] pub Value);

#[cfg(feature = "jsonschema")]
impl schemars::JsonSchema for ExtraValue {
    fn schema_name() -> String {
        Value::schema_name()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        Value::json_schema(gen)
    }

    fn is_referenceable() -> bool {
        false
    }
}

impl<T: Into<Value>> From<T> for ExtraValue {
    fn from(value: T) -> ExtraValue {
        ExtraValue(value.into())
    }
}

/// An event processing error.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct EventProcessingError {
    /// The error kind.
    #[metastructure(field = "type", required = "true")]
    pub ty: Annotated<String>,

    /// Affected key or deep path.
    pub name: Annotated<String>,

    /// The original value causing this error.
    pub value: Annotated<Value>,

    /// Additional data explaining this error.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

/// The grouping config that should be used for grouping this event.
///
/// This is currently only supplied as part of normalization and the payload
/// only permits the ID of the algorithm to be set and no parameters yet.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct GroupingConfig {
    /// The id of the grouping config.
    #[metastructure(max_chars = "enumlike")]
    pub id: Annotated<String>,
    /// The enhancements configuration.
    pub enhancements: Annotated<String>,
}

/// The sentry v7 event structure.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_event", value_type = "Event")]
pub struct Event {
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
    #[metastructure(field = "event_id")]
    pub id: Annotated<EventId>,

    /// Severity level of the event. Defaults to `error`.
    ///
    /// Example:
    ///
    /// ```json
    /// {"level": "warning"}
    /// ```
    pub level: Annotated<Level>,

    /// Version
    pub version: Annotated<String>,

    /// Type of event: error, csp, default
    #[metastructure(field = "type")]
    pub ty: Annotated<EventType>,

    /// Manual fingerprint override.
    ///
    /// A list of strings used to dictate how this event is supposed to be grouped with other
    /// events into issues. For more information about overriding grouping see [Customize Grouping
    /// with Fingerprints](https://docs.sentry.io/data-management/event-grouping/).
    ///
    /// ```json
    /// {
    ///     "fingerprint": ["myrpc", "POST", "/foo.bar"]
    /// }
    #[metastructure(skip_serialization = "empty")]
    pub fingerprint: Annotated<Fingerprint>,

    /// Custom culprit of the event.
    #[metastructure(max_chars = "culprit", pii = "maybe")]
    pub culprit: Annotated<String>,

    /// Transaction name of the event.
    ///
    /// For example, in a web app, this might be the route name (`"/users/<username>/"` or
    /// `UserView`), in a task queue it might be the function + module name.
    #[metastructure(max_chars = "culprit", trim_whitespace = "true")]
    pub transaction: Annotated<String>,

    /// Time since the start of the transaction until the error occurred.
    pub time_spent: Annotated<u64>,

    /// Custom parameterized message for this event.
    #[metastructure(legacy_alias = "sentry.interfaces.Message", legacy_alias = "message")]
    #[metastructure(skip_serialization = "empty")]
    pub logentry: Annotated<LogEntry>,

    /// Logger that created the event.
    #[metastructure(
        max_chars = "logger", // DB-imposed limit
        match_regex = r"^[^\r\n]*\z"
    )]
    pub logger: Annotated<String>,

    /// Name and versions of all installed modules/packages/dependencies in the current
    /// environment/application.
    ///
    /// ```json
    /// { "django": "3.0.0", "celery": "4.2.1" }
    /// ```
    ///
    /// In Python this is a list of installed packages as reported by `pkg_resources` together with
    /// their reported version string.
    ///
    /// This is primarily used for suggesting to enable certain SDK integrations from within the UI
    /// and for making informed decisions on which frameworks to support in future development
    /// efforts.
    #[metastructure(skip_serialization = "empty_deep", bag_size = "large")]
    pub modules: Annotated<Object<String>>,

    /// Platform identifier of this event (defaults to "other").
    ///
    /// A string representing the platform the SDK is submitting from. This will be used by the
    /// Sentry interface to customize various components in the interface, but also to enter or
    /// skip stacktrace processing.
    ///
    /// Acceptable values are: `as3`, `c`, `cfml`, `cocoa`, `csharp`, `elixir`, `haskell`, `go`,
    /// `groovy`, `java`, `javascript`, `native`, `node`, `objc`, `other`, `perl`, `php`, `python`,
    /// `ruby`
    pub platform: Annotated<String>,

    /// Timestamp when the event was created.
    ///
    /// Indicates when the event was created in the Sentry SDK. The format is either a string as
    /// defined in [RFC 3339](https://tools.ietf.org/html/rfc3339) or a numeric (integer or float)
    /// value representing the number of seconds that have elapsed since the [Unix
    /// epoch](https://en.wikipedia.org/wiki/Unix_time).
    ///
    /// Sub-microsecond precision is not preserved with numeric values due to precision
    /// limitations with floats (at least in our systems). With that caveat in mind, just send
    /// whatever is easiest to produce.
    ///
    /// All timestamps in the event protocol are formatted this way.
    ///
    /// ```json
    /// { "timestamp": "2011-05-02T17:41:36Z" }
    /// { "timestamp": 1304358096.0 }
    /// ```
    pub timestamp: Annotated<Timestamp>,

    /// Timestamp when the event has started (relevant for event type = "transaction")
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub start_timestamp: Annotated<Timestamp>,

    /// Timestamp when the event has been received by Sentry.
    pub received: Annotated<Timestamp>,

    /// Server or device name the event was generated on.
    ///
    /// This is supposed to be a hostname.
    #[metastructure(pii = "true", max_chars = "symbol")]
    pub server_name: Annotated<String>,

    /// The release version of the application.
    ///
    /// **Release versions must be unique across all projects in your organization.** This value
    /// can be the git SHA for the given project, or a product identifier with a semantic version.
    #[metastructure(
        max_chars = "tag_value",  // release ends in tag
        match_regex = r"^[^\r\n\f\t/]*\z",
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
    // Match whitespace here, which will later get trimmed
    #[metastructure(
        max_chars = "tag_value",  // dist ends in tag
        match_regex = r"^\s*[a-zA-Z0-9_.-]*\s*$",
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
        match_regex = r"^[^\r\n\x0C/]+$",
        trim_whitespace = "true"
    )]
    pub environment: Annotated<String>,

    /// Deprecated in favor of tags.
    #[metastructure(max_chars = "symbol")]
    #[metastructure(omit_from_schema)] // deprecated
    pub site: Annotated<String>,

    /// Information about the user who triggered this event.
    #[metastructure(legacy_alias = "sentry.interfaces.User")]
    #[metastructure(skip_serialization = "empty")]
    pub user: Annotated<User>,

    /// Information about a web request that occurred during the event.
    #[metastructure(legacy_alias = "sentry.interfaces.Http")]
    #[metastructure(skip_serialization = "empty")]
    pub request: Annotated<Request>,

    /// Contexts describing the environment (e.g. device, os or browser).
    #[metastructure(legacy_alias = "sentry.interfaces.Contexts")]
    pub contexts: Annotated<Contexts>,

    /// List of breadcrumbs recorded before this event.
    #[metastructure(legacy_alias = "sentry.interfaces.Breadcrumbs")]
    #[metastructure(skip_serialization = "empty")]
    pub breadcrumbs: Annotated<Values<Breadcrumb>>,

    /// One or multiple chained (nested) exceptions.
    #[metastructure(legacy_alias = "sentry.interfaces.Exception")]
    #[metastructure(field = "exception")]
    #[metastructure(skip_serialization = "empty")]
    pub exceptions: Annotated<Values<Exception>>,

    /// Event stacktrace.
    ///
    /// DEPRECATED: Prefer `threads` or `exception` depending on which is more appropriate.
    #[metastructure(skip_serialization = "empty")]
    #[metastructure(legacy_alias = "sentry.interfaces.Stacktrace")]
    pub stacktrace: Annotated<Stacktrace>,

    /// Simplified template error location information.
    /// DEPRECATED: Non-Raven clients are not supposed to send this anymore, but rather just report
    /// synthetic frames.
    #[metastructure(legacy_alias = "sentry.interfaces.Template")]
    #[metastructure(omit_from_schema)]
    pub template: Annotated<TemplateInfo>,

    /// Threads that were active when the event occurred.
    #[metastructure(skip_serialization = "empty")]
    pub threads: Annotated<Values<Thread>>,

    /// Custom tags for this event.
    ///
    /// A map or list of tags for this event. Each tag must be less than 200 characters.
    #[metastructure(skip_serialization = "empty", pii = "maybe")]
    pub tags: Annotated<Tags>,

    /// Arbitrary extra information set by the user.
    ///
    /// ```json
    /// {
    ///     "extra": {
    ///         "my_key": 1,
    ///         "some_other_value": "foo bar"
    ///     }
    /// }```
    #[metastructure(bag_size = "massive")]
    #[metastructure(pii = "true", skip_serialization = "empty")]
    pub extra: Annotated<Object<ExtraValue>>,

    /// Meta data for event processing and debugging.
    #[metastructure(skip_serialization = "empty")]
    pub debug_meta: Annotated<DebugMeta>,

    /// Information about the Sentry SDK that generated this event.
    #[metastructure(field = "sdk")]
    #[metastructure(skip_serialization = "empty")]
    pub client_sdk: Annotated<ClientSdkInfo>,

    /// Errors encountered during processing. Intended to be phased out in favor of
    /// annotation/metadata system.
    #[metastructure(skip_serialization = "empty_deep")]
    pub errors: Annotated<Array<EventProcessingError>>,

    /// Project key which sent this event.
    #[metastructure(omit_from_schema)] // not part of external schema
    pub key_id: Annotated<String>,

    /// Project which sent this event.
    #[metastructure(omit_from_schema)] // not part of external schema
    pub project: Annotated<u64>,

    /// The grouping configuration for this event.
    #[metastructure(omit_from_schema)] // not part of external schema
    pub grouping_config: Annotated<Object<Value>>,

    /// Legacy checksum used for grouping before fingerprint hashes.
    #[metastructure(max_chars = "hash")]
    #[metastructure(omit_from_schema)] // deprecated
    pub checksum: Annotated<String>,

    /// CSP (security) reports.
    #[metastructure(legacy_alias = "sentry.interfaces.Csp")]
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub csp: Annotated<Csp>,

    /// HPKP (security) reports.
    #[metastructure(pii = "true", legacy_alias = "sentry.interfaces.Hpkp")]
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub hpkp: Annotated<Hpkp>,

    /// ExpectCT (security) reports.
    #[metastructure(pii = "true", legacy_alias = "sentry.interfaces.ExpectCT")]
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub expectct: Annotated<ExpectCt>,

    /// ExpectStaple (security) reports.
    #[metastructure(pii = "true", legacy_alias = "sentry.interfaces.ExpectStaple")]
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub expectstaple: Annotated<ExpectStaple>,

    /// Spans for tracing.
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub spans: Annotated<Array<Span>>,

    /// Measurements
    #[metastructure(skip_serialization = "empty_deep")]
    pub measurements: Annotated<Measurements>,

    /// Internal ingestion and processing metrics.
    ///
    /// This value should not be ingested and will be overwritten by the store normalizer.
    #[metastructure(omit_from_schema)]
    pub _metrics: Annotated<Metrics>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

#[test]
fn test_event_roundtrip() {
    use chrono::{TimeZone, Utc};

    use crate::protocol::TagEntry;
    use crate::types::{Map, Meta};

    // NOTE: Interfaces will be tested separately.
    let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "level": "debug",
  "fingerprint": [
    "myprint"
  ],
  "culprit": "myculprit",
  "transaction": "mytransaction",
  "logentry": {
    "formatted": "mymessage"
  },
  "logger": "mylogger",
  "modules": {
    "mymodule": "1.0.0"
  },
  "platform": "myplatform",
  "timestamp": 946684800.0,
  "server_name": "myhost",
  "release": "myrelease",
  "dist": "mydist",
  "environment": "myenv",
  "tags": [
    [
      "tag",
      "value"
    ]
  ],
  "extra": {
    "extra": "value"
  },
  "other": "value",
  "_meta": {
    "event_id": {
      "": {
        "err": [
          "invalid_data"
        ]
      }
    }
  }
}"#;

    let event = Annotated::new(Event {
        id: Annotated(
            Some("52df9022-8352-46ee-b317-dbd739ccd059".parse().unwrap()),
            Meta::from_error(ErrorKind::InvalidData),
        ),
        level: Annotated::new(Level::Debug),
        fingerprint: Annotated::new(vec!["myprint".to_string()].into()),
        culprit: Annotated::new("myculprit".to_string()),
        transaction: Annotated::new("mytransaction".to_string()),
        logentry: Annotated::new(LogEntry {
            formatted: Annotated::new("mymessage".to_string().into()),
            ..Default::default()
        }),
        logger: Annotated::new("mylogger".to_string()),
        modules: {
            let mut map = Map::new();
            map.insert("mymodule".to_string(), Annotated::new("1.0.0".to_string()));
            Annotated::new(map)
        },
        platform: Annotated::new("myplatform".to_string()),
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
        server_name: Annotated::new("myhost".to_string()),
        release: Annotated::new("myrelease".to_string().into()),
        dist: Annotated::new("mydist".to_string()),
        environment: Annotated::new("myenv".to_string()),
        tags: {
            let mut items = Array::new();
            items.push(Annotated::new(TagEntry(
                Annotated::new("tag".to_string()),
                Annotated::new("value".to_string()),
            )));
            Annotated::new(Tags(items.into()))
        },
        extra: {
            let mut map = Map::new();
            map.insert(
                "extra".to_string(),
                Annotated::new(ExtraValue(Value::String("value".to_string()))),
            );
            Annotated::new(map)
        },
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, event.to_json_pretty().unwrap());
}

#[test]
fn test_event_default_values() {
    let json = "{}";
    let event = Annotated::new(Event::default());

    assert_eq_dbg!(event, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, event.to_json_pretty().unwrap());
}

#[test]
fn test_event_default_values_with_meta() {
    use crate::types::Meta;
    let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "fingerprint": [
    "{{ default }}"
  ],
  "platform": "other",
  "_meta": {
    "event_id": {
      "": {
        "err": [
          "invalid_data"
        ]
      }
    },
    "fingerprint": {
      "": {
        "err": [
          "invalid_data"
        ]
      }
    },
    "platform": {
      "": {
        "err": [
          "invalid_data"
        ]
      }
    }
  }
}"#;

    let event = Annotated::new(Event {
        id: Annotated(
            Some("52df9022-8352-46ee-b317-dbd739ccd059".parse().unwrap()),
            Meta::from_error(ErrorKind::InvalidData),
        ),
        fingerprint: Annotated(
            Some(vec!["{{ default }}".to_string()].into()),
            Meta::from_error(ErrorKind::InvalidData),
        ),
        platform: Annotated(
            Some("other".to_string()),
            Meta::from_error(ErrorKind::InvalidData),
        ),
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::<Event>::from_json(json).unwrap());
    assert_eq_str!(json, event.to_json_pretty().unwrap());
}

#[test]
fn test_event_type() {
    assert_eq_dbg!(
        EventType::Default,
        *Annotated::<EventType>::from_json("\"default\"")
            .unwrap()
            .value()
            .unwrap()
    );
}

#[test]
fn test_fingerprint_empty_string() {
    let json = r#"{"fingerprint":[""]}"#;
    let event = Annotated::new(Event {
        fingerprint: Annotated::new(vec!["".to_string()].into()),
        ..Default::default()
    });

    assert_eq_dbg!(json, event.to_json().unwrap());
    assert_eq_dbg!(event, Annotated::from_json(json).unwrap());
}

#[test]
fn test_fingerprint_null_values() {
    let input = r#"{"fingerprint":[null]}"#;
    let output = r#"{}"#;
    let event = Annotated::new(Event {
        fingerprint: Annotated::new(vec![].into()),
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_dbg!(output, event.to_json().unwrap());
}

#[test]
fn test_empty_threads() {
    let input = r#"{"threads": {}}"#;
    let output = r#"{}"#;

    let event = Annotated::new(Event::default());

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_dbg!(output, event.to_json().unwrap());
}

#[test]
fn test_lenient_release() {
    let input = r#"{"release":42}"#;
    let output = r#"{"release":"42"}"#;
    let event = Annotated::new(Event {
        release: Annotated::new("42".to_string().into()),
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_dbg!(output, event.to_json().unwrap());
}
