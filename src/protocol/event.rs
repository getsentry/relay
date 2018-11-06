use general_derive::{FromValue, ProcessValue, ToValue};

#[cfg(test)]
use chrono::TimeZone;

use super::*;

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
    // TODO: enum?
    #[metastructure(field = "type")]
    pub ty: Annotated<EventType>,

    /// Manual fingerprint override.
    pub fingerprint: Annotated<Fingerprint>,

    /// Custom culprit of the event.
    #[metastructure(cap_size = "symbol")]
    pub culprit: Annotated<String>,

    /// Transaction name of the event.
    // TODO: Cap? Is often dotted path or URL path, but could be anything
    pub transaction: Annotated<String>,

    /// Custom parameterized message for this event.
    #[metastructure(
        legacy_alias = "sentry.interfaces.Message",
        legacy_alias = "message"
    )]
    pub logentry: Annotated<LogEntry>,

    /// Logger that created the event.
    #[metastructure(cap_size = "symbol")]
    pub logger: Annotated<String>,

    /// Name and versions of installed modules.
    pub modules: Annotated<Object<String>>,

    /// Platform identifier of this event (defaults to "other").
    // TODO: Normalize null to "other"
    pub platform: Annotated<String>,

    /// Timestamp when the event was created.
    pub timestamp: Annotated<DateTime<Utc>>,

    /// Timestamp when the event has been received by Sentry.
    pub received: Annotated<DateTime<Utc>>,

    /// Server or device name the event was generated on.
    #[metastructure(pii_kind = "hostname")]
    pub server_name: Annotated<String>,

    /// Program's release identifier.
    // TODO: cap size
    pub release: Annotated<String>,

    /// Program's distribution identifier.
    // TODO: cap size
    pub dist: Annotated<String>,

    /// Environment the environment was generated in ("production" or "development").
    // TODO: cap size
    pub environment: Annotated<String>,

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
    // TODO: Cap? (trim_dict in python)
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
    pub key_id: Annotated<String>,

    /// Project key which sent this event.
    pub project: Annotated<u64>,

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
            items.push(Annotated::new((
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
