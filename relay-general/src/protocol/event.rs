use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use failure::Fail;
use serde::{Deserialize, Serialize, Serializer};

#[cfg(test)]
use chrono::TimeZone;

use crate::processor::ProcessValue;
use crate::protocol::{
    Breadcrumb, ClientSdkInfo, Contexts, Csp, DebugMeta, Exception, ExpectCt, ExpectStaple,
    Fingerprint, Hpkp, LenientString, Level, LogEntry, Metrics, Request, Span, Stacktrace, Tags,
    TemplateInfo, Thread, User, Values,
};
use crate::types::{
    Annotated, Array, Empty, ErrorKind, FromValue, Object, SkipSerialization, ToValue, Value,
};

/// Wrapper around a UUID with slightly different formatting.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, PiiStrippable, SchemaValidated,
)]
pub struct EventId(pub uuid::Uuid);

impl EventId {
    /// Creates a new event id using a UUID v4.
    #[inline]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
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

/// The type of event we're dealing with.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    PiiStrippable,
    SchemaValidated,
)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Default,
    Error,
    Csp,
    Hpkp,
    ExpectCT,
    ExpectStaple,
    Transaction,
}

/// An error used when parsing `EventType`.
#[derive(Debug, Fail)]
#[fail(display = "invalid event type")]
pub struct ParseEventTypeError;

impl Default for EventType {
    fn default() -> Self {
        EventType::Default
    }
}

impl FromStr for EventType {
    type Err = ParseEventTypeError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "default" => EventType::Default,
            "error" => EventType::Error,
            "csp" => EventType::Csp,
            "hpkp" => EventType::Hpkp,
            "expectct" => EventType::ExpectCT,
            "expectstaple" => EventType::ExpectStaple,
            "transaction" => EventType::Transaction,
            _ => return Err(ParseEventTypeError),
        })
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            EventType::Default => write!(f, "default"),
            EventType::Error => write!(f, "error"),
            EventType::Csp => write!(f, "csp"),
            EventType::Hpkp => write!(f, "hpkp"),
            EventType::ExpectCT => write!(f, "expectct"),
            EventType::ExpectStaple => write!(f, "expectstaple"),
            EventType::Transaction => write!(f, "transaction"),
        }
    }
}

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

#[derive(
    Debug, FromValue, ToValue, ProcessValue, PiiStrippable, SchemaValidated, Empty, Clone, PartialEq,
)]
pub struct ExtraValue(#[metastructure(bag_size = "larger")] pub Value);

impl<T: Into<Value>> From<T> for ExtraValue {
    fn from(value: T) -> ExtraValue {
        ExtraValue(value.into())
    }
}

/// An event processing error.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    ToValue,
    ProcessValue,
    PiiStrippable,
    SchemaValidated,
)]
pub struct EventProcessingError {
    #[rename = "type"]
    #[required]
    /// The error kind.
    pub ty: Annotated<String>,

    /// Affected key or deep path.
    pub name: Annotated<String>,

    /// The original value causing this error.
    pub value: Annotated<Value>,

    /// Additional data explaining this error.
    #[metastructure(additional_properties)]
    #[should_strip_pii = true]
    pub other: Object<Value>,
}

/// The grouping config that should be used for grouping this event.
///
/// This is currently only supplied as part of normalization and the payload
/// only permits the ID of the algorithm to be set and no parameters yet.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    ToValue,
    ProcessValue,
    PiiStrippable,
    SchemaValidated,
)]
pub struct GroupingConfig {
    /// The id of the grouping config.
    #[metastructure(max_chars = "enumlike")]
    pub id: Annotated<String>,
    /// The enhancements configuration.
    pub enhancements: Annotated<String>,
}

/// The sentry v7 event structure.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    ToValue,
    ProcessValue,
    SchemaValidated,
    PiiStrippable,
)]
#[metastructure(process_func = "process_event", value_type = "Event")]
pub struct Event {
    /// Unique identifier of this event.
    #[rename = "event_id"]
    pub id: Annotated<EventId>,

    /// Severity level of the event.
    pub level: Annotated<Level>,

    /// Version
    pub version: Annotated<String>,

    /// Type of event: error, csp, default
    #[rename = "type"]
    pub ty: Annotated<EventType>,

    /// Manual fingerprint override.
    #[metastructure(skip_serialization = "empty")]
    pub fingerprint: Annotated<Fingerprint>,

    /// Custom culprit of the event.
    #[metastructure(max_chars = "culprit")]
    pub culprit: Annotated<String>,

    /// Transaction name of the event.
    #[metastructure(max_chars = "culprit")]
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
    )]
    #[match_regex = r"^[^\r\n]*\z"]
    pub logger: Annotated<String>,

    /// Name and versions of installed modules.
    #[metastructure(skip_serialization = "empty_deep", bag_size = "large")]
    pub modules: Annotated<Object<String>>,

    /// Platform identifier of this event (defaults to "other").
    pub platform: Annotated<String>,

    /// Timestamp when the event was created.
    pub timestamp: Annotated<DateTime<Utc>>,

    /// Timestamp when the event has started (relevant for event type = "transaction")
    pub start_timestamp: Annotated<DateTime<Utc>>,

    /// Timestamp when the event has been received by Sentry.
    pub received: Annotated<DateTime<Utc>>,

    /// Server or device name the event was generated on.
    #[metastructure(max_chars = "symbol")]
    #[should_strip_pii = true]
    pub server_name: Annotated<String>,

    /// Program's release identifier.
    #[metastructure(
        max_chars = "tag_value",  // release ends in tag
        skip_serialization = "empty"
    )]
    #[nonempty]
    #[trim_whitespace]
    #[match_regex = r"^[^\r\n]*\z"]
    pub release: Annotated<LenientString>,

    /// Program's distribution identifier.
    // Match whitespace here, which will later get trimmed
    #[metastructure(max_chars = "tag_value")] // dist ends in tag
    #[nonempty]
    #[match_regex = r"^\s*[a-zA-Z0-9_.-]*\s*$"]
    pub dist: Annotated<String>,

    /// Environment the environment was generated in ("production" or "development").
    #[metastructure(max_chars = "environment")]
    #[trim_whitespace]
    #[match_regex = r"^[^\r\n\x0C/]+$"]
    pub environment: Annotated<String>,

    /// Deprecated in favor of tags.
    #[metastructure(max_chars = "symbol")]
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
    #[should_strip_pii = true]
    pub contexts: Annotated<Contexts>,

    /// List of breadcrumbs recorded before this event.
    #[metastructure(legacy_alias = "sentry.interfaces.Breadcrumbs")]
    #[metastructure(skip_serialization = "empty")]
    pub breadcrumbs: Annotated<Values<Breadcrumb>>,

    /// One or multiple chained (nested) exceptions.
    #[metastructure(legacy_alias = "sentry.interfaces.Exception")]
    #[rename = "exception"]
    #[metastructure(skip_serialization = "empty")]
    pub exceptions: Annotated<Values<Exception>>,

    /// Deprecated event stacktrace.
    #[metastructure(skip_serialization = "empty")]
    #[metastructure(legacy_alias = "sentry.interfaces.Stacktrace")]
    pub stacktrace: Annotated<Stacktrace>,

    /// Simplified template error location information.
    #[metastructure(legacy_alias = "sentry.interfaces.Template")]
    pub template: Annotated<TemplateInfo>,

    /// Threads that were active when the event occurred.
    #[metastructure(skip_serialization = "empty")]
    pub threads: Annotated<Values<Thread>>,

    /// Custom tags for this event.
    #[metastructure(skip_serialization = "empty")]
    pub tags: Annotated<Tags>,

    /// Arbitrary extra information set by the user.
    #[metastructure(bag_size = "massive")]
    #[metastructure(skip_serialization = "empty")]
    #[should_strip_pii = true]
    pub extra: Annotated<Object<ExtraValue>>,

    /// Meta data for event processing and debugging.
    #[metastructure(skip_serialization = "empty")]
    pub debug_meta: Annotated<DebugMeta>,

    /// Information about the Sentry SDK that generated this event.
    #[rename = "sdk"]
    #[metastructure(skip_serialization = "empty")]
    pub client_sdk: Annotated<ClientSdkInfo>,

    /// Errors encountered during processing. Intended to be phased out in favor of
    /// annotation/metadata system.
    #[metastructure(skip_serialization = "empty_deep")]
    pub errors: Annotated<Array<EventProcessingError>>,

    /// Project key which sent this event.
    // TODO: capsize?
    pub key_id: Annotated<String>,

    /// Project which sent this event.
    pub project: Annotated<u64>,

    /// The grouping configuration for this event.
    pub grouping_config: Annotated<Object<Value>>,

    /// Legacy checksum used for grouping before fingerprint hashes.
    #[metastructure(max_chars = "hash")]
    pub checksum: Annotated<String>,

    /// CSP (security) reports.
    #[metastructure(legacy_alias = "sentry.interfaces.Csp")]
    pub csp: Annotated<Csp>,

    /// HPKP (security) reports.
    #[metastructure(legacy_alias = "sentry.interfaces.Hpkp")]
    #[should_strip_pii = true]
    pub hpkp: Annotated<Hpkp>,

    /// ExpectCT (security) reports.
    #[metastructure(legacy_alias = "sentry.interfaces.ExpectCT")]
    #[should_strip_pii = true]
    pub expectct: Annotated<ExpectCt>,

    /// ExpectStaple (security) reports.
    #[metastructure(legacy_alias = "sentry.interfaces.ExpectStaple")]
    #[should_strip_pii = true]
    pub expectstaple: Annotated<ExpectStaple>,

    /// Spans for tracing.
    pub spans: Annotated<Array<Span>>,

    /// Internal ingestion and processing metrics.
    ///
    /// This value should not be ingested and will be overwritten by the store normalizer.
    pub _metrics: Annotated<Metrics>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    #[should_strip_pii = true]
    pub other: Object<Value>,
}

#[test]
fn test_event_roundtrip() {
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
            formatted: Annotated::new("mymessage".to_string()),
            ..Default::default()
        }),
        logger: Annotated::new("mylogger".to_string()),
        modules: {
            let mut map = Map::new();
            map.insert("mymodule".to_string(), Annotated::new("1.0.0".to_string()));
            Annotated::new(map)
        },
        platform: Annotated::new("myplatform".to_string()),
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
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
