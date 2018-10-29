use chrono::{DateTime, Utc};
use uuid::Uuid;

use meta::{Annotated, Value};
use types::{Array, Level, Object, Values};

#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    /// Unique identifier of this event.
    #[metastructure(field = "event_id")]
    pub id: Annotated<Uuid>,

    /// Severity level of the event.
    pub level: Annotated<Level>,

    /// Manual fingerprint override.
    //TODO: implement this
    //#[serde(skip_serializing_if = "event::is_default_fingerprint")]
    //pub fingerprint: Annotated<Vec<String>>,

    /// Custom culprit of the event.
    pub culprit: Annotated<String>,

    /// Transaction name of the event.
    pub transaction: Annotated<String>,

    /// Custom message for this event.
    // TODO: Consider to normalize this right away into logentry
    #[metastructure(pii_kind = "freeform", cap_size = "message")]
    pub message: Annotated<String>,

    /// Custom parameterized message for this event.
    #[metastructure(legacy_alias = "sentry.interfaces.Message")]
    pub logentry: Annotated<LogEntry>,

    /// Logger that created the event.
    pub logger: Annotated<String>,

    /// Name and versions of installed modules.
    pub modules: Annotated<String>,

    /// Platform identifier of this event (defaults to "other").
    pub platform: Annotated<String>,

    /// Timestamp when the event was created.
    pub timestamp: Annotated<DateTime<Utc>>,

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
    // pub contexts: Annotated<Object<Context>>,

    /// List of breadcrumbs recorded before this event.
    //#[metastructure(legacy_alias = "sentry.interfaces.Breadcrumbs")]
    // pub breadcrumbs: Annotated<Values<Breadcrumb>>,

    /// One or multiple chained (nested) exceptions.
    //#[metastructure(legacy_alias = "sentry.interfaces.Exception")]
    #[metastructure(field = "exception")]
    pub exceptions: Annotated<Values<Exception>>,

    /// Deprecated event stacktrace.
    pub stacktrace: Annotated<Stacktrace>,

    /// Simplified template error location information.
    // #[metastructure(name = "template", legacy_alias = "sentry.interfaces.Template")]
    // pub template_info: Annotated<Option<TemplateInfo>>,

    /// Threads that were active when the event occurred.
    pub threads: Annotated<Values<Thread>>,

    /// Custom tags for this event.
    // pub tags: Annotated<Object<String>>,

    /// Approximate geographical location of the end user or device.
    pub geo: Annotated<Geo>,

    /// Arbitrary extra information set by the user.
    pub extra: Annotated<Object<Value>>,

    /// Meta data for event processing and debugging.
    pub debug_meta: Annotated<DebugMeta>,

    /// Information about the Sentry SDK that generated this event.
    #[metastructure(field = "sdk")]
    pub client_sdk: Annotated<ClientSdkInfo>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
    pub other: Object<Value>,
}

/// Information about the user who triggered an event.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct User {
    /// Unique identifier of the user.
    #[metastructure(pii_kind = "id")]
    pub id: Annotated<String>,

    /// Email address of the user.
    #[metastructure(pii_kind = "email")]
    pub email: Annotated<String>,

    /// Remote IP address of the user. Defaults to "{{auto}}".
    #[metastructure(pii_kind = "ip")]
    pub ip_address: Annotated<String>,

    /// Human readable name of the user.
    #[metastructure(pii_kind = "username")]
    pub username: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
    pub other: Object<Value>,
}

/// A log entry message.
///
/// A log message is similar to the `message` attribute on the event itself but
/// can additionally hold optional parameters.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct LogEntry {
    /// The log message with parameter placeholders (required).
    #[metastructure(pii_kind = "freeform", cap_size = "message")]
    pub message: Annotated<String>,

    /// Positional parameters to be interpolated into the log message.
    #[metastructure(pii_kind = "databag")]
    pub params: Annotated<Array<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
    pub other: Object<Value>,
}

/// A map holding cookies.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct Cookies(pub Object<String>);

/// A map holding headers.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct Headers(pub Object<String>);

/// A map holding query string pairs.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct Query(pub Object<String>);

/// Http request information.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct Request {
    /// URL of the request.
    // TODO: cap?
    pub url: Annotated<String>,

    /// HTTP request method.
    pub method: Annotated<String>,

    /// Request data in any format that makes sense.
    // TODO: cap?
    // TODO: Custom logic + info
    #[metastructure(pii_kind = "databag")]
    pub data: Annotated<Value>,

    /// URL encoded HTTP query string.
    #[metastructure(pii_kind = "databag")]
    pub query_string: Annotated<Query>,

    /// URL encoded contents of the Cookie header.
    #[metastructure(pii_kind = "databag")]
    pub cookies: Annotated<Cookies>,

    /// HTTP request headers.
    #[metastructure(pii_kind = "databag")]
    pub headers: Annotated<Headers>,

    /// Server environment data, such as CGI/WSGI.
    #[metastructure(pii_kind = "databag")]
    pub env: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_stacktrace")]
pub struct Stacktrace {
    pub frames: Annotated<Array<Frame>>,
}

#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_frame")]
pub struct Frame {
    pub function: Annotated<String>,
}

#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_exception")]
pub struct Exception {
    #[metastructure(field = "type", required = "true")]
    pub ty: Annotated<String>,
    #[metastructure(cap_size = "summary")]
    pub value: Annotated<String>,
    #[metastructure(cap_size = "symbol")]
    pub module: Annotated<String>,
    #[metastructure(legacy_alias = "sentry.interfaces.Stacktrace")]
    pub stacktrace: Annotated<Stacktrace>,
    pub raw_stacktrace: Annotated<Stacktrace>,
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Information about the Sentry SDK.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct ClientSdkInfo {
    /// Unique SDK name.
    pub name: Annotated<String>,

    /// SDK version.
    pub version: Annotated<String>,

    /// List of integrations that are enabled in the SDK.
    pub integrations: Annotated<Array<String>>,

    /// List of installed and loaded SDK packages.
    pub packages: Annotated<Array<ClientSdkPackage>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// An installed and loaded package as part of the Sentry SDK.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct ClientSdkPackage {
    /// Name of the package.
    pub name: Annotated<String>,
    /// Version of the package.
    pub version: Annotated<String>,
}

/// Debugging and processing meta information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct DebugMeta {
    /// Information about the system SDK (e.g. iOS SDK).
    #[metastructure(field = "sdk_info")]
    pub system_sdk: Annotated<SystemSdkInfo>,

    /// List of debug information files (debug images).
    pub images: Annotated<Array<DebugImage>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Holds information about the system SDK.
///
/// This is relevant for iOS and other platforms that have a system
/// SDK.  Not to be confused with the client SDK.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct SystemSdkInfo {
    /// The internal name of the SDK.
    pub sdk_name: Annotated<String>,

    /// The major version of the SDK as integer or 0.
    pub version_major: Annotated<u64>,

    /// The minor version of the SDK as integer or 0.
    pub version_minor: Annotated<u64>,

    /// The patch version of the SDK as integer or 0.
    pub version_patchlevel: Annotated<u64>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

// TODO: Figure out DebugImage
type DebugImage = Object<Value>;

/// Geographical location of the end user or device.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct Geo {
    /// Two-letter country code (ISO 3166-1 alpha-2).
    #[metastructure(pii_kind = "location")]
    // TODO: cap=summary?
    pub country_code: Annotated<String>,

    /// Human readable city name.
    #[metastructure(pii_kind = "location")]
    // TODO: cap=summary?
    pub city: Annotated<String>,

    /// Human readable region name or code.
    #[metastructure(pii_kind = "location")]
    // TODO: cap=summary?
    pub region: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A process thread of an event.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct Thread {
    /// Identifier of this thread within the process (usually an integer).
    pub id: Annotated<ThreadId>,

    /// Display name of this thread.
    // TODO: cap=summary?
    pub name: Annotated<String>,

    /// Stack trace containing frames of this exception.
    pub stacktrace: Annotated<Stacktrace>,

    /// Optional unprocessed stack trace.
    pub raw_stacktrace: Annotated<Stacktrace>,

    /// Indicates that this thread requested the event (usually by crashing).
    pub crashed: Annotated<bool>,

    /// Indicates that the thread was not suspended when the event was created.
    pub current: Annotated<bool>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Identifier of a thread within an event.
// TODO: Figure out untagged enums
type ThreadId = Value;

#[test]
fn test_user_roundtrip() {
    let json = r#"{
  "id": "e4e24881-8238-4539-a32b-d3c3ecd40568",
  "email": "mail@example.org",
  "ip_address": "{{auto}}",
  "username": "John Doe",
  "other": "value"
}"#;
    let user = Annotated::new(User {
        id: Annotated::new("e4e24881-8238-4539-a32b-d3c3ecd40568".to_string()),
        email: Annotated::new("mail@example.org".to_string()),
        ip_address: Annotated::new("{{auto}}".to_string()),
        username: Annotated::new("John Doe".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(user, Annotated::<User>::from_json(json).unwrap());
    assert_eq_str!(json, user.to_json_pretty().unwrap());
}

#[test]
fn test_logentry_roundtrip() {
    let json = r#"{
  "message": "Hello, %s %s!",
  "params": [
    "World",
    1
  ],
  "other": "value"
}"#;

    let entry = Annotated::new(LogEntry {
        message: Annotated::new("Hello, %s %s!".to_string()),
        params: Annotated::new(vec![
            Annotated::new(Value::String("World".to_string())),
            Annotated::new(Value::I64(1)),
        ]),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(entry, Annotated::<LogEntry>::from_json(json).unwrap());
    assert_eq_str!(json, entry.to_json_pretty().unwrap());
}
