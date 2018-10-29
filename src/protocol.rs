use chrono::{DateTime, Utc};
use cookie::Cookie;
use serde_json;
use url::form_urlencoded;
use uuid::Uuid;

use meta::{Annotated, Value};
use processor::{FromKey, FromValue, ToKey};
use types::{Addr, Array, Level, Map, Object, RegVal, ThreadId, Values};

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
    // pub contexts: Annotated<Contexts>,

    /// List of breadcrumbs recorded before this event.
    #[metastructure(legacy_alias = "sentry.interfaces.Breadcrumbs")]
    pub breadcrumbs: Annotated<Values<Breadcrumb>>,

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
    #[metastructure(
        pii_kind = "freeform",
        cap_size = "message",
        required = "true"
    )]
    pub message: Annotated<String>,

    /// Positional parameters to be interpolated into the log message.
    #[metastructure(pii_kind = "databag")]
    pub params: Annotated<Array<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
    pub other: Object<Value>,
}

/// A map holding cookies.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Cookies(pub Object<String>);

impl FromValue for Cookies {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                let mut cookies = Map::new();
                for cookie in value.split(";") {
                    match Cookie::parse_encoded(cookie) {
                        Ok(cookie) => {
                            cookies.insert(
                                cookie.name().to_string(),
                                Annotated::new(cookie.value().to_string()),
                            );
                        }
                        Err(err) => {
                            meta.add_error(err.to_string());
                        }
                    }
                }
                Annotated(Some(Cookies(cookies)), meta)
            }
            annotated @ Annotated(Some(Value::Object(_)), _) => {
                let Annotated(value, meta) = FromValue::from_value(annotated);
                Annotated(value.map(Cookies), meta)
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected cookies");
                Annotated(None, meta)
            }
        }
    }
}

/// A map holding headers.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct Headers(pub Map<HeaderKey, HeaderValue>);

/// The key of an HTTP header.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct HeaderKey(String);

/// The value of an HTTP header.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, FromValue, ToValue, ProcessValue)]
pub struct HeaderValue(String);

impl ToKey for HeaderKey {
    #[inline(always)]
    fn to_key(key: HeaderKey) -> String {
        key.0
    }
}

impl FromKey for HeaderKey {
    #[inline(always)]
    fn from_key(key: String) -> HeaderKey {
        HeaderKey(
            key.split('-')
                .enumerate()
                .fold(String::new(), |mut all, (i, part)| {
                    // join
                    if i > 0 {
                        all.push_str("-");
                    }

                    // capitalize the first characters
                    let mut chars = part.chars();
                    if let Some(c) = chars.next() {
                        all.extend(c.to_uppercase());
                    }

                    // copy all others
                    all.extend(chars);
                    all
                }),
        )
    }
}

/// A map holding query string pairs.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Query(pub Object<String>);

impl FromValue for Query {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(v)), meta) => {
                let mut rv = Object::new();
                let qs = if v.starts_with('?') { &v[1..] } else { &v[..] };
                for (key, value) in form_urlencoded::parse(qs.as_bytes()) {
                    rv.insert(key.to_string(), Annotated::new(value.to_string()));
                }
                Annotated(Some(Query(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(Query(
                    items
                        .into_iter()
                        .map(|(k, v)| match v {
                            v @ Annotated(Some(Value::String(_)), _)
                            | v @ Annotated(Some(Value::Null), _) => {
                                (FromKey::from_key(k), FromValue::from_value(v))
                            }
                            v => {
                                let v = match v {
                                    v @ Annotated(Some(Value::Object(_)), _)
                                    | v @ Annotated(Some(Value::Array(_)), _) => {
                                        let meta = v.1.clone();
                                        let json_val: serde_json::Value = v.into();
                                        Annotated(
                                            Some(Value::String(
                                                serde_json::to_string(&json_val).unwrap(),
                                            )),
                                            meta,
                                        )
                                    }
                                    other => other,
                                };
                                (FromKey::from_key(k), FromValue::from_value(v))
                            }
                        }).collect(),
                )),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected query");
                Annotated(None, meta)
            }
        }
    }
}

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

    /// Register values of the thread (top frame).
    pub registers: Annotated<Object<RegVal>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_frame")]
pub struct Frame {
    /// Name of the frame's function. This might include the name of a class.
    pub function: Annotated<String>,

    /// Potentially mangled name of the symbol as it appears in an executable.
    ///
    /// This is different from a function name by generally being the mangled
    /// name that appears natively in the binary.  This is relevant for languages
    /// like Swift, C++ or Rust.
    pub symbol: Annotated<String>,

    /// Name of the module the frame is contained in.
    ///
    /// Note that this might also include a class name if that is something the
    /// language natively considers to be part of the stack (for instance in Java).
    #[metastructure(pii_kind = "freeform")]
    // TODO: Cap? This can be a FS path or a dotted path
    pub module: Annotated<String>,

    /// Name of the package that contains the frame.
    ///
    /// For instance this can be a dylib for native languages, the name of the jar
    /// or .NET assembly.
    #[metastructure(pii_kind = "freeform")]
    // TODO: Cap? This can be a FS path or a dotted path
    pub package: Annotated<String>,

    /// The source file name (basename only).
    #[metastructure(pii_kind = "freeform", cap_size = "short_path")]
    pub filename: Annotated<String>,

    /// Absolute path to the source file.
    #[metastructure(pii_kind = "freeform", cap_size = "path")]
    pub abs_path: Annotated<String>,

    /// Line number within the source file.
    #[metastructure(field = "lineno")]
    pub line: Annotated<u64>,

    /// Column number within the source file.
    #[metastructure(field = "colno")]
    pub column: Annotated<u64>,

    /// Source code leading up to the current line.
    #[metastructure(field = "pre_context")]
    pub pre_lines: Annotated<Array<String>>,

    /// Source code of the current line.
    #[metastructure(field = "context_line")]
    pub current_line: Annotated<String>,

    /// Source code of the lines after the current line.
    #[metastructure(field = "post_context")]
    pub post_lines: Annotated<Array<String>>,

    /// Override whether this frame should be considered in-app.
    pub in_app: Annotated<bool>,

    /// Local variables in a convenient format.
    #[metastructure(pii_kind = "databag")]
    pub vars: Annotated<Object<Value>>,

    /// Start address of the containing code module (image).
    pub image_addr: Annotated<Addr>,

    /// Absolute address of the frame's CPU instruction.
    pub instruction_addr: Annotated<Addr>,

    /// Start address of the frame's function.
    pub symbol_addr: Annotated<Addr>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_exception")]
pub struct Exception {
    /// Exception type (required).
    #[metastructure(field = "type", required = "true")]
    pub ty: Annotated<String>,

    /// Human readable display value.
    #[metastructure(cap_size = "summary")]
    pub value: Annotated<String>,

    /// Module name of this exception.
    #[metastructure(cap_size = "symbol")]
    pub module: Annotated<String>,

    /// Stack trace containing frames of this exception.
    #[metastructure(legacy_alias = "sentry.interfaces.Stacktrace")]
    pub stacktrace: Annotated<Stacktrace>,

    /// Optional unprocessed stack trace.
    pub raw_stacktrace: Annotated<Stacktrace>,

    /// Identifier of the thread this exception occurred in.
    pub thread_id: Annotated<ThreadId>,

    /// Mechanism by which this exception was generated and handled.
    pub mechanism: Annotated<Mechanism>,

    /// Additional arbitrary fields for forwards compatibility.
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
    #[metastructure(pii_kind = "location", cap_size = "summary")]
    pub country_code: Annotated<String>,

    /// Human readable city name.
    #[metastructure(pii_kind = "location", cap_size = "summary")]
    pub city: Annotated<String>,

    /// Human readable region name or code.
    #[metastructure(pii_kind = "location", cap_size = "summary")]
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
    #[metastructure(cap_size = "summary")]
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

/// Contexts describing the environment (e.g. device, os or browser).
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
struct Contexts {
    device: Annotated<Box<DeviceContext>>,
    os: Annotated<Box<OsContext>>,
    runtime: Annotated<Box<RuntimeContext>>,
    app: Annotated<Box<AppContext>>,
    browser: Annotated<Box<BrowserContext>>,
}

/// Device information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct DeviceContext {
    /// Name of the device.
    pub name: Annotated<String>,

    /// Family of the device model.
    pub family: Annotated<String>,

    /// Device model (human readable).
    pub model: Annotated<String>,

    /// Device model (internal identifier).
    pub model_id: Annotated<String>,

    /// Native cpu architecture of the device.
    pub arch: Annotated<String>,

    /// Current battery level (0-100).
    pub battery_level: Annotated<f64>,

    /// Current screen orientation.
    pub orientation: Annotated<String>,

    /// Simulator/prod indicator.
    pub simulator: Annotated<bool>,

    /// Total memory available in bytes.
    pub memory_size: Annotated<u64>,

    /// How much memory is still available in bytes.
    pub free_memory: Annotated<u64>,

    /// How much memory is usable for the app in bytes.
    pub usable_memory: Annotated<u64>,

    /// Total storage size of the device in bytes.
    pub storage_size: Annotated<u64>,

    /// How much storage is free in bytes.
    pub free_storage: Annotated<u64>,

    /// Total size of the attached external storage in bytes (eg: android SDK card).
    pub external_storage_size: Annotated<u64>,

    /// Free size of the attached external storage in bytes (eg: android SDK card).
    pub external_free_storage: Annotated<u64>,

    /// Indicator when the device was booted.
    pub boot_time: Annotated<DateTime<Utc>>,

    /// Timezone of the device.
    pub timezone: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Operating system information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct OsContext {
    /// Name of the operating system.
    #[metastructure(cap_size = "summary")]
    pub name: Annotated<String>,

    /// Version of the operating system.
    #[metastructure(cap_size = "summary")]
    pub version: Annotated<String>,

    /// Internal build number of the operating system.
    #[metastructure(cap_size = "summary")]
    pub build: Annotated<String>,

    /// Current kernel version.
    #[metastructure(cap_size = "summary")]
    pub kernel_version: Annotated<String>,

    /// Indicator if the OS is rooted (mobile mostly).
    pub rooted: Annotated<bool>,

    /// Unprocessed operating system info.
    #[metastructure(cap_size = "summary")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Runtime information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct RuntimeContext {
    /// Runtime name.
    #[metastructure(cap_size = "summary")]
    pub name: Annotated<String>,

    /// Runtime version.
    #[metastructure(cap_size = "summary")]
    pub version: Annotated<String>,

    /// Unprocessed runtime info.
    #[metastructure(cap_size = "summary")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Application information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct AppContext {
    /// Start time of the app.
    pub app_start_time: Annotated<DateTime<Utc>>,

    /// Device app hash (app specific device ID)
    #[metastructure(pii_kind = "id")]
    #[metastructure(cap_size = "summary")]
    pub device_app_hash: Annotated<String>,

    /// Build identicator.
    #[metastructure(cap_size = "summary")]
    pub build_type: Annotated<String>,

    /// App identifier (dotted bundle id).
    pub app_identifier: Annotated<String>,

    /// Application name as it appears on the platform.
    pub app_name: Annotated<String>,

    /// Application version as it appears on the platform.
    pub app_version: Annotated<String>,

    /// Internal build ID as it appears on the platform.
    #[metastructure(cap_size = "summary")]
    pub app_build: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Web browser information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct BrowserContext {
    /// Runtime name.
    #[metastructure(cap_size = "summary")]
    pub name: Annotated<String>,

    /// Runtime version.
    #[metastructure(cap_size = "summary")]
    pub version: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A breadcrumb.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct Breadcrumb {
    /// The timestamp of the breadcrumb (required).
    pub timestamp: Annotated<DateTime<Utc>>,

    /// The type of the breadcrumb.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,

    /// The optional category of the breadcrumb.
    pub category: Annotated<String>,

    /// Severity level of the breadcrumb (required).
    pub level: Annotated<Level>,

    /// Human readable message for the breadcrumb.
    #[metastructure(pii_kind = "freeform", cap_size = "message")]
    pub message: Annotated<String>,

    /// Custom user-defined data of this breadcrumb.
    #[metastructure(pii_kind = "databag")]
    pub data: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// The mechanism by which an exception was generated and handled.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct Mechanism {
    /// Mechanism type (required).
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,

    /// Human readable detail description.
    #[metastructure(pii_kind = "freeform", cap_size = "message")]
    pub description: Annotated<String>,

    /// Link to online resources describing this error.
    pub help_link: Annotated<String>,

    /// Flag indicating whether this exception was handled.
    pub handled: Annotated<bool>,

    /// Additional attributes depending on the mechanism type.
    #[metastructure(pii_kind = "databag")]
    pub data: Annotated<Object<Value>>,

    /// Operating system or runtime meta information.
    pub meta: Annotated<MechanismMeta>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Operating system or runtime meta information to an exception mechanism.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct MechanismMeta {
    /// Optional ISO C standard error code.
    pub errno: Annotated<CError>,

    /// Optional POSIX signal number.
    pub signal: Annotated<PosixSignal>,

    /// Optional mach exception information.
    pub mach_exception: Annotated<MachException>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// POSIX signal with optional extended data.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct CError {
    /// The error code as specified by ISO C99, POSIX.1-2001 or POSIX.1-2008.
    pub number: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,
}

/// Mach exception information.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct MachException {
    /// The mach exception type.
    #[metastructure(field = "exception")]
    pub ty: Annotated<i64>,

    /// The mach exception code.
    pub code: Annotated<u64>,

    /// The mach exception subcode.
    pub subcode: Annotated<u64>,

    /// Optional name of the mach exception.
    pub name: Annotated<String>,
}

/// POSIX signal with optional extended data.
#[derive(Debug, Clone, FromValue, ToValue, ProcessValue)]
pub struct PosixSignal {
    /// The POSIX signal number.
    pub number: Annotated<i64>,

    /// An optional signal code present on Apple systems.
    pub code: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,

    /// Optional name of the errno constant.
    pub code_name: Annotated<String>,
}

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
