use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use failure::Fail;
use serde::ser::{Serialize, Serializer};

#[cfg(test)]
use chrono::TimeZone;

use crate::processor::ProcessValue;
use crate::protocol::{
    Breadcrumb, ClientSdkInfo, Contexts, DebugMeta, Exception, Fingerprint, Level, LogEntry,
    Request, Stacktrace, Tags, TemplateInfo, Thread, User, Values,
};
use crate::types::{Annotated, Array, FromValue, Object, ToValue, Value};

/// Wrapper around a UUID with slightly different formatting.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub uuid::Uuid);
primitive_meta_structure_through_string!(EventId, "event id");

impl ProcessValue for EventId {}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_simple_ref())
    }
}

impl FromStr for EventId {
    type Err = uuid::parser::ParseError;

    fn from_str(uuid_str: &str) -> Result<Self, Self::Err> {
        uuid_str.parse().map(EventId)
    }
}

/// The type of event we're dealing with.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EventType {
    Default,
    Error,
    Csp,
    Hpkp,
    ExpectCT,
    ExpectStaple,
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
            _ => return Err(ParseEventTypeError),
        })
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            EventType::Default => write!(f, "default"),
            EventType::Error => write!(f, "error"),
            EventType::Csp => write!(f, "csp"),
            EventType::Hpkp => write!(f, "hpkp"),
            EventType::ExpectCT => write!(f, "expectct"),
            EventType::ExpectStaple => write!(f, "expectstaple"),
        }
    }
}

impl FromValue for EventType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match <String as FromValue>::from_value(value) {
            Annotated(Some(value), meta) => match EventType::from_str(&value) {
                Ok(x) => Annotated(Some(x), meta),
                Err(_) => Annotated::from_error("invalid event type", None),
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ToValue for EventType {
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(value), meta) => {
                Annotated(Some(Value::String(format!("{}", value))), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for EventType {}

/// An event processing error.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct EventProcessingError {
    #[metastructure(field = "type", required = "true")]
    /// Error type, see src/sentry/models/eventerror.py
    pub ty: Annotated<String>,

    /// Affected key
    pub name: Annotated<String>,

    /// Faulty value
    pub value: Annotated<Value>,
}

/// The sentry v7 event structure.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    /// Unique identifier of this event.
    #[metastructure(field = "event_id")]
    pub id: Annotated<EventId>,

    /// Severity level of the event.
    pub level: Annotated<Level>,

    /// Version
    pub version: Annotated<String>,

    /// Type of event: error, csp, default
    #[metastructure(field = "type")]
    pub ty: Annotated<EventType>,

    /// Manual fingerprint override.
    pub fingerprint: Annotated<Fingerprint>,

    /// Custom culprit of the event.
    #[metastructure(max_chars = "symbol")]
    pub culprit: Annotated<String>,

    /// Transaction name of the event.
    // TODO: Is this the right cap? Is often dotted path or URL path, but could be anything
    #[metastructure(max_chars = "symbol")]
    pub transaction: Annotated<String>,

    /// Custom parameterized message for this event.
    #[metastructure(
        legacy_alias = "sentry.interfaces.Message",
        legacy_alias = "message"
    )]
    pub logentry: Annotated<LogEntry>,

    /// Logger that created the event.
    #[metastructure(max_chars = "symbol", match_regex = r"^[^\r\n]+\z")]
    pub logger: Annotated<String>,

    /// Name and versions of installed modules.
    pub modules: Annotated<Object<String>>,

    /// Platform identifier of this event (defaults to "other").
    pub platform: Annotated<String>,

    /// Timestamp when the event was created.
    pub timestamp: Annotated<DateTime<Utc>>,

    /// Timestamp when the event has been received by Sentry.
    pub received: Annotated<DateTime<Utc>>,

    /// Server or device name the event was generated on.
    #[metastructure(pii_kind = "hostname", max_chars = "symbol")]
    pub server_name: Annotated<String>,

    /// Program's release identifier.
    #[metastructure(max_chars = "symbol", match_regex = r"^[^\r\n]*\z")]
    pub release: Annotated<String>,

    /// Program's distribution identifier.
    // Match whitespace here, which will later get trimmed
    #[metastructure(
        max_chars = "symbol",
        match_regex = r"^\s*[a-zA-Z0-9_.-]+\s*$"
    )]
    pub dist: Annotated<String>,

    /// Environment the environment was generated in ("production" or "development").
    #[metastructure(max_chars = "enumlike", match_regex = r"^[^\r\n\x0C/]+$")]
    pub environment: Annotated<String>,

    /// Deprecated in favor of tags
    #[metastructure(max_chars = "symbol")]
    pub site: Annotated<String>,

    /// Information about the user who triggered this event.
    #[metastructure(legacy_alias = "sentry.interfaces.User")]
    pub user: Annotated<User>,

    /// Information about a web request that occurred during the event.
    #[metastructure(legacy_alias = "sentry.interfaces.Http")]
    pub request: Annotated<Request>,

    /// Contexts describing the environment (e.g. device, os or browser).
    #[metastructure(legacy_alias = "sentry.interfaces.Contexts")]
    pub contexts: Annotated<Contexts>,

    /// List of breadcrumbs recorded before this event.
    #[metastructure(legacy_alias = "sentry.interfaces.Breadcrumbs")]
    pub breadcrumbs: Annotated<Values<Breadcrumb>>,

    /// One or multiple chained (nested) exceptions.
    #[metastructure(legacy_alias = "sentry.interfaces.Exception")]
    #[metastructure(field = "exception")]
    pub exceptions: Annotated<Values<Exception>>,

    /// Deprecated event stacktrace.
    pub stacktrace: Annotated<Stacktrace>,

    /// Simplified template error location information.
    #[metastructure(legacy_alias = "sentry.interfaces.Template")]
    pub template_info: Annotated<TemplateInfo>,

    /// Threads that were active when the event occurred.
    pub threads: Annotated<Values<Thread>>,

    /// Custom tags for this event.
    pub tags: Annotated<Tags>,

    /// Arbitrary extra information set by the user.
    #[metastructure(bag_size = "large")]
    pub extra: Annotated<Object<Value>>,

    /// Meta data for event processing and debugging.
    pub debug_meta: Annotated<DebugMeta>,

    /// Information about the Sentry SDK that generated this event.
    #[metastructure(field = "sdk")]
    pub client_sdk: Annotated<ClientSdkInfo>,

    /// Errors encountered during processing. Intended to be phased out in favor of
    /// annotation/metadata system.
    pub errors: Annotated<Array<EventProcessingError>>,

    /// Project key which sent this event.
    // TODO: capsize?
    pub key_id: Annotated<String>,

    /// Project which sent this event.
    pub project: Annotated<u64>,

    /// Project which sent this event.
    // TODO: capsize?
    pub checksum: Annotated<String>,

    /// CSP (security) reports.
    // TODO: typing
    #[metastructure(legacy_alias = "sentry.interfaces.Csp")]
    pub csp: Annotated<Value>,

    /// HPKP (security) reports.
    // TODO: typing
    #[metastructure(legacy_alias = "sentry.interfaces.Hpkp")]
    pub hpkp: Annotated<Value>,

    /// ExpectCT (security) reports.
    // TODO: typing
    #[metastructure(legacy_alias = "sentry.interfaces.ExpectCT")]
    pub expectct: Annotated<Value>,

    /// ExpectStaple (security) reports.
    // TODO: typing
    #[metastructure(legacy_alias = "sentry.interfaces.ExpectStaple")]
    pub expectstaple: Annotated<Value>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
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
          "some error"
        ]
      }
    }
  }
}"#;

    let event = Annotated::new(Event {
        id: Annotated(
            Some("52df9022-8352-46ee-b317-dbd739ccd059".parse().unwrap()),
            {
                let mut meta = Meta::default();
                meta.add_error("some error", None);
                meta
            },
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
        release: Annotated::new("myrelease".to_string()),
        dist: Annotated::new("mydist".to_string()),
        environment: Annotated::new("myenv".to_string()),
        tags: {
            let mut items = Array::new();
            items.push(Annotated::new(TagEntry(
                Annotated::new("tag".to_string()),
                Annotated::new("value".to_string()),
            )));
            Annotated::new(Tags(items))
        },
        extra: {
            let mut map = Map::new();
            map.insert(
                "extra".to_string(),
                Annotated::new(Value::String("value".to_string())),
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
          "some error"
        ]
      }
    },
    "fingerprint": {
      "": {
        "err": [
          "some error"
        ]
      }
    },
    "platform": {
      "": {
        "err": [
          "some error"
        ]
      }
    }
  }
}"#;

    let event = Annotated::new(Event {
        id: Annotated(
            Some("52df9022-8352-46ee-b317-dbd739ccd059".parse().unwrap()),
            {
                let mut meta = Meta::default();
                meta.add_error("some error", None);
                meta
            },
        ),
        fingerprint: Annotated(Some(vec!["{{ default }}".to_string()].into()), {
            let mut meta = Meta::default();
            meta.add_error("some error", None);
            meta
        }),
        platform: Annotated(Some("other".to_string()), {
            let mut meta = Meta::default();
            meta.add_error("some error", None);
            meta
        }),
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::<Event>::from_json(json).unwrap());
    assert_eq_str!(json, event.to_json_pretty().unwrap());
}

#[test]
fn test_event_type() {
    assert_eq_dbg!(
        EventType::Default,
        Annotated::<EventType>::from_json("\"default\"")
            .unwrap()
            .0
            .unwrap()
    );
}
