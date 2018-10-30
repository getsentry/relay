use chrono::{DateTime, Utc};
use cookie::Cookie;
use debugid::DebugId;
use serde::{Serialize, Serializer};
use serde_json;
use url::form_urlencoded;
use uuid::Uuid;

use general_derive::{FromValue, ProcessValue, ToValue};

use crate::meta::{Annotated, MetaMap, Value};
use crate::processor::{FromKey, FromValue, ProcessValue, ToKey, ToValue};
use crate::types::{Addr, Array, Level, Map, Object, ObjectOrArray, RegVal, ThreadId, Values};

#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    /// Unique identifier of this event.
    #[metastructure(field = "event_id")]
    pub id: Annotated<Uuid>,

    /// Severity level of the event.
    pub level: Annotated<Level>,

    /// Manual fingerprint override.
    pub fingerprint: Annotated<Fingerprint>,

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
    pub modules: Annotated<Object<String>>,

    /// Platform identifier of this event (defaults to "other").
    // TODO: Normalize null to "other"
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
    #[metastructure(legacy_alias = "sentry.interfaces.Template")]
    pub template_info: Annotated<TemplateInfo>,

    /// Threads that were active when the event occurred.
    pub threads: Annotated<Values<Thread>>,

    /// Custom tags for this event.
    pub tags: Annotated<ObjectOrArray<String>>,

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

#[derive(Debug, Clone, PartialEq)]
pub struct Fingerprint(Vec<String>);

impl From<Vec<String>> for Fingerprint {
    fn from(vec: Vec<String>) -> Fingerprint {
        Fingerprint(vec)
    }
}

impl FromValue for Fingerprint {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(Value::Array(array)), mut meta) => {
                let mut fingerprint = vec![];
                for elem in array {
                    match elem {
                        Annotated(Some(Value::String(string)), _) => fingerprint.push(string),
                        Annotated(Some(Value::I64(int)), _) => fingerprint.push(int.to_string()),
                        Annotated(Some(Value::U64(int)), _) => fingerprint.push(int.to_string()),
                        Annotated(Some(Value::F64(float)), _) => {
                            if float.abs() < (1i64 << 53) as f64 {
                                fingerprint.push(float.trunc().to_string());
                            }
                        }
                        Annotated(Some(Value::Bool(true)), _) => {
                            fingerprint.push("True".to_string())
                        }
                        Annotated(Some(Value::Bool(false)), _) => {
                            fingerprint.push("False".to_string())
                        }
                        Annotated(value_opt, meta2) => {
                            for err in meta2.iter_errors() {
                                meta.add_error(err, None);
                            }

                            if let Some(value) = value_opt {
                                meta.add_error(
                                    format!(
                                        "expected number, string or bool, got {}",
                                        value.describe()
                                    ),
                                    Some(value),
                                );
                            } else if !meta.has_errors() {
                                meta.add_error("value required", None);
                            }

                            return Annotated(None, meta);
                        }
                    }
                }
                Annotated(Some(Fingerprint(fingerprint)), meta)
            }
            Annotated(Some(value), mut meta) => {
                meta.add_error(
                    format!("expected array, found {}", value.describe()),
                    Some(value),
                );
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ToValue for Fingerprint {
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(value), mut meta) => match serde_json::to_value(value.0) {
                Ok(value) => Annotated(Some(value.into()), meta),
                Err(err) => {
                    meta.add_error(err.to_string(), None);
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    #[inline(always)]
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.0, s)
    }
}

// TODO: implement normalization s.t. ["{{ default }}"] => null
impl ProcessValue for Fingerprint {}

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
                for cookie in value.split(';') {
                    if cookie.trim().is_empty() {
                        continue;
                    }
                    match Cookie::parse_encoded(cookie) {
                        Ok(cookie) => {
                            cookies.insert(
                                cookie.name().to_string(),
                                Annotated::new(cookie.value().to_string()),
                            );
                        }
                        Err(err) => {
                            meta.add_error(
                                err.to_string(),
                                Some(Value::String(cookie.to_string())),
                            );
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
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("cookies", value);
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
pub struct HeaderKey(pub String);

/// The value of an HTTP header.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, FromValue, ToValue, ProcessValue)]
pub struct HeaderValue(pub String);

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
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("query-string or map", value);
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

#[derive(Debug, Clone, Default, PartialEq, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_stacktrace")]
pub struct Stacktrace {
    pub frames: Annotated<Array<Frame>>,

    /// Register values of the thread (top frame).
    pub registers: Annotated<Object<RegVal>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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

#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct ClientSdkPackage {
    /// Name of the package.
    pub name: Annotated<String>,
    /// Version of the package.
    pub version: Annotated<String>,
}

/// Debugging and processing meta information.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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

/// Apple debug image in
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct AppleDebugImage {
    /// The static type of this debug image.
    #[metastructure(field = "type", required = "true")]
    ty: Annotated<String>,

    /// Path and name of the debug image (required).
    #[metastructure(required = "true")]
    pub name: Annotated<String>,

    /// CPU architecture target.
    pub arch: Annotated<String>,

    /// MachO CPU type identifier.
    pub cpu_type: Annotated<u64>,

    /// MachO CPU subtype identifier.
    pub cpu_subtype: Annotated<u64>,

    /// Starting memory address of the image (required).
    #[metastructure(required = "true")]
    pub image_addr: Annotated<Addr>,

    /// Size of the image in bytes (required).
    #[metastructure(required = "true")]
    pub image_size: Annotated<u64>,

    /// Loading address in virtual memory.
    pub image_vmaddr: Annotated<Addr>,

    /// The unique UUID of the image.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Any debug information file supported by symbolic.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct SymbolicDebugImage {
    /// The static type of this debug image.
    #[metastructure(field = "type", required = "true")]
    ty: Annotated<String>,

    /// Path and name of the debug image (required).
    #[metastructure(required = "true")]
    pub name: Annotated<String>,

    /// CPU architecture target.
    pub arch: Annotated<String>,

    /// Starting memory address of the image (required).
    #[metastructure(required = "true")]
    pub image_addr: Annotated<Addr>,

    /// Size of the image in bytes (required).
    #[metastructure(required = "true")]
    pub image_size: Annotated<u64>,

    /// Loading address in virtual memory.
    pub image_vmaddr: Annotated<Addr>,

    /// Unique debug identifier of the image.
    #[metastructure(required = "true")]
    pub id: Annotated<DebugId>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Proguard mapping file.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct ProguardDebugImage {
    /// The static type of this debug image.
    #[metastructure(field = "type", required = "true")]
    ty: Annotated<String>,

    /// UUID computed from the file contents.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A debug information file (debug image).
#[derive(Debug, Clone, PartialEq)]
pub enum DebugImage {
    /// Apple debug images (machos).  This is currently also used for non apple platforms with
    /// similar debug setups.
    Apple(Box<AppleDebugImage>),
    /// Symbolic (new style) debug infos.
    Symbolic(Box<SymbolicDebugImage>),
    /// A reference to a proguard debug file.
    Proguard(Box<ProguardDebugImage>),
    /// A debug image that is unknown to this protocol specification.
    Other(Object<Value>),
}

impl FromValue for DebugImage {
    fn from_value(annotated: Annotated<Value>) -> Annotated<Self> {
        match Object::<Value>::from_value(annotated) {
            Annotated(Some(object), meta) => match object
                .get("type")
                .and_then(|a| a.0.as_ref())
                .and_then(|v| v.as_str())
                .unwrap_or_default()
            {
                "apple" => {
                    AppleDebugImage::from_value(Annotated(Some(Value::Object(object)), meta))
                        .map_value(|i| DebugImage::Apple(Box::new(i)))
                }
                "symbolic" => {
                    SymbolicDebugImage::from_value(Annotated(Some(Value::Object(object)), meta))
                        .map_value(|i| DebugImage::Symbolic(Box::new(i)))
                }
                "proguard" => {
                    ProguardDebugImage::from_value(Annotated(Some(Value::Object(object)), meta))
                        .map_value(|i| DebugImage::Proguard(Box::new(i)))
                }
                _ => Annotated(Some(DebugImage::Other(object)), meta),
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ToValue for DebugImage {
    fn to_value(annotated: Annotated<Self>) -> Annotated<Value> {
        match annotated {
            Annotated(Some(image), meta) => match image {
                DebugImage::Apple(inner) => ToValue::to_value(Annotated(Some(*inner), meta)),
                DebugImage::Symbolic(inner) => ToValue::to_value(Annotated(Some(*inner), meta)),
                DebugImage::Proguard(inner) => ToValue::to_value(Annotated(Some(*inner), meta)),
                DebugImage::Other(inner) => ToValue::to_value(Annotated(Some(inner), meta)),
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn extract_child_meta(&self) -> MetaMap {
        match self {
            DebugImage::Apple(inner) => inner.extract_child_meta(),
            DebugImage::Symbolic(inner) => inner.extract_child_meta(),
            DebugImage::Proguard(inner) => inner.extract_child_meta(),
            DebugImage::Other(inner) => inner.extract_child_meta(),
        }
    }

    fn serialize_payload<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DebugImage::Apple(inner) => inner.serialize_payload(serializer),
            DebugImage::Symbolic(inner) => inner.serialize_payload(serializer),
            DebugImage::Proguard(inner) => inner.serialize_payload(serializer),
            DebugImage::Other(inner) => inner.serialize_payload(serializer),
        }
    }
}

impl ProcessValue for DebugImage {
    fn process_value<P: crate::processor::Processor>(
        annotated: Annotated<Self>,
        processor: &P,
        state: crate::processor::ProcessingState,
    ) -> Annotated<Self> {
        match annotated {
            Annotated(Some(image), meta) => match image {
                DebugImage::Apple(inner) => {
                    ProcessValue::process_value(Annotated(Some(*inner), meta), processor, state)
                        .map_value(|i| DebugImage::Apple(Box::new(i)))
                }
                DebugImage::Symbolic(inner) => {
                    ProcessValue::process_value(Annotated(Some(*inner), meta), processor, state)
                        .map_value(|i| DebugImage::Symbolic(Box::new(i)))
                }
                DebugImage::Proguard(inner) => {
                    ProcessValue::process_value(Annotated(Some(*inner), meta), processor, state)
                        .map_value(|i| DebugImage::Proguard(Box::new(i)))
                }
                DebugImage::Other(inner) => {
                    ProcessValue::process_value(Annotated(Some(inner), meta), processor, state)
                        .map_value(DebugImage::Other)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

/// Geographical location of the end user or device.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
struct Contexts {
    device: Annotated<Box<DeviceContext>>,
    os: Annotated<Box<OsContext>>,
    runtime: Annotated<Box<RuntimeContext>>,
    app: Annotated<Box<AppContext>>,
    browser: Annotated<Box<BrowserContext>>,
}

/// Device information.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
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

impl FromValue for Mechanism {
    fn from_value(annotated: Annotated<Value>) -> Annotated<Self> {
        #[derive(FromValue)]
        struct NewMechanism {
            #[metastructure(field = "type", required = "true")]
            pub ty: Annotated<String>,
            pub description: Annotated<String>,
            pub help_link: Annotated<String>,
            pub handled: Annotated<bool>,
            pub data: Annotated<Object<Value>>,
            pub meta: Annotated<MechanismMeta>,
            #[metastructure(additional_properties)]
            pub other: Object<Value>,
        }

        #[derive(FromValue)]
        struct LegacyPosixSignal {
            #[metastructure(required = "true")]
            pub signal: Annotated<i64>,
            pub code: Annotated<i64>,
            pub name: Annotated<String>,
            pub code_name: Annotated<String>,
        }

        #[derive(FromValue)]
        struct LegacyMachException {
            #[metastructure(required = "true")]
            pub exception: Annotated<i64>,
            #[metastructure(required = "true")]
            pub code: Annotated<u64>,
            #[metastructure(required = "true")]
            pub subcode: Annotated<u64>,
            pub exception_name: Annotated<String>,
        }

        #[derive(FromValue)]
        struct LegacyMechanism {
            posix_signal: Annotated<LegacyPosixSignal>,
            mach_exception: Annotated<LegacyMachException>,
            #[metastructure(additional_properties)]
            pub other: Object<Value>,
        }

        match annotated {
            Annotated(Some(Value::Object(object)), meta) => {
                if object.is_empty() {
                    Annotated(None, meta)
                } else if object.contains_key("type") {
                    let annotated = Annotated(Some(Value::Object(object)), meta);
                    NewMechanism::from_value(annotated).map_value(|mechanism| Mechanism {
                        ty: mechanism.ty,
                        description: mechanism.description,
                        help_link: mechanism.help_link,
                        handled: mechanism.handled,
                        data: mechanism.data,
                        meta: mechanism.meta,
                        other: mechanism.other,
                    })
                } else {
                    let annotated = Annotated(Some(Value::Object(object)), meta);
                    LegacyMechanism::from_value(annotated).map_value(|legacy| Mechanism {
                        ty: Annotated::new("generic".to_string()),
                        description: Annotated::empty(),
                        help_link: Annotated::empty(),
                        handled: Annotated::empty(),
                        data: Annotated::new(legacy.other),
                        meta: Annotated::new(MechanismMeta {
                            errno: Annotated::empty(),
                            signal: legacy.posix_signal.map_value(|legacy| PosixSignal {
                                number: legacy.signal,
                                code: legacy.code,
                                name: legacy.name,
                                code_name: legacy.code_name,
                            }),
                            mach_exception: legacy.mach_exception.map_value(|legacy| {
                                MachException {
                                    ty: legacy.exception,
                                    code: legacy.code,
                                    subcode: legacy.subcode,
                                    name: legacy.exception_name,
                                }
                            }),
                            other: Object::default(),
                        }),
                        other: Object::default(),
                    })
                }
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("exception mechanism", value);
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

/// Operating system or runtime meta information to an exception mechanism.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct CError {
    /// The error code as specified by ISO C99, POSIX.1-2001 or POSIX.1-2008.
    #[metastructure(required = "true")]
    pub number: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,
}

/// Mach exception information.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct MachException {
    /// The mach exception type.
    #[metastructure(field = "exception", required = "true")]
    pub ty: Annotated<i64>,

    /// The mach exception code.
    #[metastructure(required = "true")]
    pub code: Annotated<u64>,

    /// The mach exception subcode.
    #[metastructure(required = "true")]
    pub subcode: Annotated<u64>,

    /// Optional name of the mach exception.
    pub name: Annotated<String>,
}

/// POSIX signal with optional extended data.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct PosixSignal {
    /// The POSIX signal number.
    #[metastructure(required = "true")]
    pub number: Annotated<i64>,

    /// An optional signal code present on Apple systems.
    pub code: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,

    /// Optional name of the errno constant.
    pub code_name: Annotated<String>,
}

/// Template debug information.
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
pub struct TemplateInfo {
    /// The file name (basename only).
    #[metastructure(pii_kind = "freeform", cap_size = "short_path")]
    pub filename: Annotated<String>,

    /// Absolute path to the file.
    #[metastructure(pii_kind = "freeform", cap_size = "path")]
    pub abs_path: Annotated<String>,

    /// Line number within the source file.
    pub lineno: Annotated<u64>,

    /// Column number within the source file.
    pub colno: Annotated<u64>,

    /// Source code leading up to the current line.
    pub pre_context: Annotated<Array<String>>,

    /// Source code of the current line.
    pub context_line: Annotated<String>,

    /// Source code of the lines after the current line.
    pub post_context: Annotated<Array<String>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[test]
fn test_debug_image_proguard_roundtrip() {
    let json = r#"{
  "type": "proguard",
  "uuid": "395835f4-03e0-4436-80d3-136f0749a893",
  "other": "value"
}"#;
    let image = Annotated::new(DebugImage::Proguard(Box::new(ProguardDebugImage {
        ty: Annotated::new("proguard".to_string()),
        uuid: Annotated::new(
            "395835f4-03e0-4436-80d3-136f0749a893"
                .parse::<Uuid>()
                .unwrap(),
        ),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_apple_roundtrip() {
    let json = r#"{
  "type": "apple",
  "name": "CoreFoundation",
  "arch": "arm64",
  "cpu_type": 1233,
  "cpu_subtype": 3,
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "uuid": "494f3aea-88fa-4296-9644-fa8ef5d139b6",
  "other": "value"
}"#;

    let image = Annotated::new(DebugImage::Apple(Box::new(AppleDebugImage {
        ty: Annotated::new("apple".to_string()),
        name: Annotated::new("CoreFoundation".to_string()),
        arch: Annotated::new("arm64".to_string()),
        cpu_type: Annotated::new(1233),
        cpu_subtype: Annotated::new(3),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
        uuid: Annotated::new(
            "494f3aea-88fa-4296-9644-fa8ef5d139b6"
                .parse::<Uuid>()
                .unwrap(),
        ),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_apple_default_values() {
    let json = r#"{
  "type": "apple",
  "name": "CoreFoundation",
  "image_addr": "0x0",
  "image_size": 4096,
  "uuid": "494f3aea-88fa-4296-9644-fa8ef5d139b6"
}"#;

    let image = Annotated::new(DebugImage::Apple(Box::new(AppleDebugImage {
        ty: Annotated::new("apple".to_string()),
        name: Annotated::new("CoreFoundation".to_string()),
        arch: Annotated::empty(),
        cpu_type: Annotated::empty(),
        cpu_subtype: Annotated::empty(),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::empty(),
        uuid: Annotated::new(
            "494f3aea-88fa-4296-9644-fa8ef5d139b6"
                .parse::<Uuid>()
                .unwrap(),
        ),
        other: Object::default(),
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_symbolic_roundtrip() {
    let json = r#"{
  "type": "symbolic",
  "name": "CoreFoundation",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234",
  "other": "value"
}"#;

    let image = Annotated::new(DebugImage::Symbolic(Box::new(SymbolicDebugImage {
        ty: Annotated::new("symbolic".to_string()),
        name: Annotated::new("CoreFoundation".to_string()),
        arch: Annotated::new("arm64".to_string()),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
        id: Annotated::new(
            "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234"
                .parse::<DebugId>()
                .unwrap(),
        ),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_symbolic_default_values() {
    let json = r#"{
  "type": "symbolic",
  "name": "CoreFoundation",
  "image_addr": "0x0",
  "image_size": 4096,
  "id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234"
}"#;

    let image = Annotated::new(DebugImage::Symbolic(Box::new(SymbolicDebugImage {
        ty: Annotated::new("symbolic".to_string()),
        name: Annotated::new("CoreFoundation".to_string()),
        arch: Annotated::empty(),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::empty(),
        id: Annotated::new(
            "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234"
                .parse::<DebugId>()
                .unwrap(),
        ),
        other: Object::default(),
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_other_roundtrip() {
    let json = r#"{"other":"value","type":"mytype"}"#;
    let image = Annotated::new(DebugImage::Other({
        let mut map = Map::new();
        map.insert(
            "type".to_string(),
            Annotated::new(Value::String("mytype".to_string())),
        );
        map.insert(
            "other".to_string(),
            Annotated::new(Value::String("value".to_string())),
        );
        map
    }));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json().unwrap());
}

#[test]
fn test_debug_image_untagged_roundtrip() {
    let json = r#"{"other":"value"}"#;
    let image = Annotated::new(DebugImage::Other({
        let mut map = Map::new();
        map.insert(
            "other".to_string(),
            Annotated::new(Value::String("value".to_string())),
        );
        map
    }));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json().unwrap());
}

#[test]
fn test_template_roundtrip() {
    let json = r#"{
  "filename": "myfile.rs",
  "abs_path": "/path/to",
  "lineno": 2,
  "colno": 42,
  "pre_context": [
    "fn main() {"
  ],
  "context_line": "unimplemented!()",
  "post_context": [
    "}"
  ],
  "other": "value"
}"#;
    let template_info = Annotated::new(TemplateInfo {
        filename: Annotated::new("myfile.rs".to_string()),
        abs_path: Annotated::new("/path/to".to_string()),
        lineno: Annotated::new(2),
        colno: Annotated::new(42),
        pre_context: Annotated::new(vec![Annotated::new("fn main() {".to_string())]),
        context_line: Annotated::new("unimplemented!()".to_string()),
        post_context: Annotated::new(vec![Annotated::new("}".to_string())]),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(template_info, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, template_info.to_json_pretty().unwrap());
}

#[test]
fn test_template_default_values() {
    let json = "{}";
    let template_info = Annotated::new(TemplateInfo {
        filename: Annotated::empty(),
        abs_path: Annotated::empty(),
        lineno: Annotated::empty(),
        colno: Annotated::empty(),
        pre_context: Annotated::empty(),
        context_line: Annotated::empty(),
        post_context: Annotated::empty(),
        other: Object::default(),
    });

    assert_eq_dbg!(template_info, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, template_info.to_json().unwrap());
}

#[test]
fn test_mechanism_roundtrip() {
    let json = r#"{
  "type": "mytype",
  "description": "mydescription",
  "help_link": "https://developer.apple.com/library/content/qa/qa1367/_index.html",
  "handled": false,
  "data": {
    "relevant_address": "0x1"
  },
  "meta": {
    "errno": {
      "number": 2,
      "name": "ENOENT"
    },
    "signal": {
      "number": 11,
      "code": 0,
      "name": "SIGSEGV",
      "code_name": "SEGV_NOOP"
    },
    "mach_exception": {
      "exception": 1,
      "code": 1,
      "subcode": 8,
      "name": "EXC_BAD_ACCESS"
    },
    "other": "value"
  },
  "other": "value"
}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        description: Annotated::new("mydescription".to_string()),
        help_link: Annotated::new(
            "https://developer.apple.com/library/content/qa/qa1367/_index.html".to_string(),
        ),
        handled: Annotated::new(false),
        data: {
            let mut map = Map::new();
            map.insert(
                "relevant_address".to_string(),
                Annotated::new(Value::String("0x1".to_string())),
            );
            Annotated::new(map)
        },
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::new(2),
                name: Annotated::new("ENOENT".to_string()),
            }),
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(1),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                name: Annotated::new("EXC_BAD_ACCESS".to_string()),
            }),
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                name: Annotated::new("SIGSEGV".to_string()),
                code_name: Annotated::new("SEGV_NOOP".to_string()),
            }),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        }),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, mechanism.to_json_pretty().unwrap());
}

#[test]
fn test_mechanism_default_values() {
    let json = r#"{"type":"mytype"}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        description: Annotated::empty(),
        help_link: Annotated::empty(),
        handled: Annotated::empty(),
        data: Annotated::empty(),
        meta: Annotated::empty(),
        other: Object::default(),
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, mechanism.to_json().unwrap());
}

#[test]
fn test_mechanism_empty() {
    let mechanism = Annotated::<Mechanism>::empty();
    assert_eq_dbg!(mechanism, Annotated::from_json("{}").unwrap());
}

#[test]
fn test_mechanism_invalid_meta() {
    let json = r#"{
  "type":"mytype",
  "meta": {
    "errno": {"name": "ENOENT"},
    "mach_exception": {"name": "EXC_BAD_ACCESS"},
    "signal": {"name": "SIGSEGV"}
  }
}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        description: Annotated::empty(),
        help_link: Annotated::empty(),
        handled: Annotated::empty(),
        data: Annotated::empty(),
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::from_error("value required", None),
                name: Annotated::new("ENOENT".to_string()),
            }),
            mach_exception: Annotated::new(MachException {
                ty: Annotated::from_error("value required", None),
                code: Annotated::from_error("value required", None),
                subcode: Annotated::from_error("value required", None),
                name: Annotated::new("EXC_BAD_ACCESS".to_string()),
            }),
            signal: Annotated::new(PosixSignal {
                number: Annotated::from_error("value required", None),
                code: Annotated::empty(),
                name: Annotated::new("SIGSEGV".to_string()),
                code_name: Annotated::empty(),
            }),
            other: Default::default(),
        }),
        other: Object::default(),
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
}

#[test]
fn test_mechanism_legacy_conversion() {
    let input = r#"{
  "posix_signal": {
    "name": "SIGSEGV",
    "code_name": "SEGV_NOOP",
    "signal": 11,
    "code": 0
  },
  "relevant_address": "0x1",
  "mach_exception": {
    "exception": 1,
    "exception_name": "EXC_BAD_ACCESS",
    "subcode": 8,
    "code": 1
  }
}"#;

    let output = r#"{
  "type": "generic",
  "data": {
    "relevant_address": "0x1"
  },
  "meta": {
    "signal": {
      "number": 11,
      "code": 0,
      "name": "SIGSEGV",
      "code_name": "SEGV_NOOP"
    },
    "mach_exception": {
      "exception": 1,
      "code": 1,
      "subcode": 8,
      "name": "EXC_BAD_ACCESS"
    }
  }
}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("generic".to_string()),
        description: Annotated::empty(),
        help_link: Annotated::empty(),
        handled: Annotated::empty(),
        data: {
            let mut map = Map::new();
            map.insert(
                "relevant_address".to_string(),
                Annotated::new(Value::String("0x1".to_string())),
            );
            Annotated::new(map)
        },
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::empty(),
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(1),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                name: Annotated::new("EXC_BAD_ACCESS".to_string()),
            }),
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                name: Annotated::new("SIGSEGV".to_string()),
                code_name: Annotated::new("SEGV_NOOP".to_string()),
            }),
            other: Object::default(),
        }),
        other: Object::default(),
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, mechanism.to_json_pretty().unwrap());
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

    assert_eq_dbg!(user, Annotated::from_json(json).unwrap());
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

    assert_eq_dbg!(entry, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, entry.to_json_pretty().unwrap());
}

#[test]
fn test_request_roundtrip() {
    let json = r#"{
  "url": "https://google.com/search",
  "method": "GET",
  "data": {
    "some": 1
  },
  "query_string": {
    "q": "foo"
  },
  "cookies": {
    "GOOGLE": "1"
  },
  "headers": {
    "Referer": "https://google.com/"
  },
  "env": {
    "REMOTE_ADDR": "213.47.147.207"
  },
  "other": "value"
}"#;

    let request = Annotated::new(Request {
        url: Annotated::new("https://google.com/search".to_string()),
        method: Annotated::new("GET".to_string()),
        data: {
            let mut map = Object::new();
            map.insert("some".to_string(), Annotated::new(Value::I64(1)));
            Annotated::new(Value::Object(map))
        },
        query_string: Annotated::new(Query({
            let mut map = Object::new();
            map.insert("q".to_string(), Annotated::new("foo".to_string()));
            map
        })),
        cookies: Annotated::new(Cookies({
            let mut map = Map::new();
            map.insert("GOOGLE".to_string(), Annotated::new("1".to_string()));
            map
        })),
        headers: Annotated::new(Headers({
            let mut map = Map::new();
            map.insert(
                HeaderKey("Referer".to_string()),
                Annotated::new(HeaderValue("https://google.com/".to_string())),
            );
            map
        })),
        env: Annotated::new({
            let mut map = Object::new();
            map.insert(
                "REMOTE_ADDR".to_string(),
                Annotated::new(Value::String("213.47.147.207".to_string())),
            );
            map
        }),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(request, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, request.to_json_pretty().unwrap());
}

#[test]
fn test_query_string() {
    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar\"").unwrap());
    assert_eq_dbg!(query, Annotated::from_json("\"?foo=bar\"").unwrap());

    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    map.insert("baz".to_string(), Annotated::new("42".to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar&baz=42\"").unwrap());
}

#[test]
fn test_query_string_legacy_nested() {
    // this test covers a case that previously was let through the ingest system but in a bad
    // way.  This was untyped and became a str repr() in Python.  New SDKs will no longer send
    // nested objects here but for legacy values we instead serialize it out as JSON.
    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar\"").unwrap());

    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    map.insert("baz".to_string(), Annotated::new(r#"{"a":42}"#.to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(
        query,
        Annotated::from_json(
            r#"
        {
            "foo": "bar",
            "baz": {"a": 42}
        }
    "#
        ).unwrap()
    );
}

#[test]
fn test_query_invalid() {
    let query =
        Annotated::<Query>::from_error("expected query-string or map", Some(Value::U64(64)));
    assert_eq_dbg!(query, Annotated::from_json("42").unwrap());
}

#[test]
fn test_cookies_parsing() {
    let json = "\" PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;\"";

    let mut map = Map::new();
    map.insert(
        "PHPSESSID".to_string(),
        Annotated::new("298zf09hf012fh2".to_string()),
    );
    map.insert(
        "csrftoken".to_string(),
        Annotated::new("u32t4o3tb3gg43".to_string()),
    );
    map.insert("_gat".to_string(), Annotated::new("1".to_string()));

    let cookies = Annotated::new(Cookies(map));
    assert_eq_dbg!(cookies, Annotated::from_json(json).unwrap());
}

#[test]
fn test_cookies_object() {
    let json = r#"{"foo":"bar", "invalid": 42}"#;

    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    map.insert(
        "invalid".to_string(),
        Annotated::from_error("expected a string", Some(Value::U64(42))),
    );

    let cookies = Annotated::new(Cookies(map));
    assert_eq_dbg!(cookies, Annotated::from_json(json).unwrap());
}

#[cfg(test)]
mod test_event {
    use chrono::{TimeZone, Utc};
    use crate::meta::*;
    use crate::protocol::*;

    #[test]
    fn test_roundtrip() {
        // NOTE: Interfaces will be tested separately.
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "level": "debug",
  "fingerprint": [
    "myprint"
  ],
  "culprit": "myculprit",
  "transaction": "mytransaction",
  "message": "mymessage",
  "logger": "mylogger",
  "modules": {
    "mymodule": "1.0.0"
  },
  "platform": "myplatform",
  "timestamp": 946684800,
  "server_name": "myhost",
  "release": "myrelease",
  "dist": "mydist",
  "environment": "myenv",
  "tags": {
    "tag": "value"
  },
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
            message: Annotated::new("mymessage".to_string()),
            logentry: Annotated::empty(),
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
            user: Annotated::empty(),
            request: Annotated::empty(),
            //contexts: Default::default(), TODO
            breadcrumbs: Annotated::empty(),
            exceptions: Annotated::empty(),
            stacktrace: Annotated::empty(),
            template_info: Annotated::empty(),
            threads: Annotated::empty(),
            tags: {
                let mut map = Map::new();
                map.insert("tag".to_string(), Annotated::new("value".to_string()));
                Annotated::new(ObjectOrArray(map))
            },
            geo: Annotated::empty(),
            extra: {
                let mut map = Map::new();
                map.insert(
                    "extra".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                Annotated::new(map)
            },
            debug_meta: Annotated::empty(),
            client_sdk: Annotated::empty(),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        });

        assert_eq_dbg!(event, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, event.to_json_pretty().unwrap());
    }

    #[test]
    fn test_default_values() {
        let json = "{}";
        let event = Annotated::new(Event {
            id: Annotated::empty(),
            level: Annotated::empty(),
            fingerprint: Annotated::new(vec!["{{ default }}".to_string()].into()),
            culprit: Annotated::empty(),
            transaction: Annotated::empty(),
            message: Annotated::empty(),
            logentry: Annotated::empty(),
            logger: Annotated::empty(),
            modules: Annotated::empty(),
            platform: Annotated::new("other".to_string()),
            timestamp: Annotated::empty(),
            server_name: Annotated::empty(),
            release: Annotated::empty(),
            dist: Annotated::empty(),
            environment: Annotated::empty(),
            user: Annotated::empty(),
            request: Annotated::empty(),
            //contexts: Default::default(), TODO
            breadcrumbs: Annotated::empty(),
            exceptions: Annotated::empty(),
            stacktrace: Annotated::empty(),
            template_info: Annotated::empty(),
            threads: Annotated::empty(),
            tags: Annotated::empty(),
            geo: Annotated::empty(),
            extra: Annotated::empty(),
            debug_meta: Annotated::empty(),
            client_sdk: Annotated::empty(),
            other: Default::default(),
        });

        assert_eq_dbg!(event, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, event.to_json_pretty().unwrap());
    }

    #[test]
    fn test_default_values_with_meta() {
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
            level: Annotated::empty(),
            fingerprint: Annotated(Some(vec!["{{ default }}".to_string()].into()), {
                let mut meta = Meta::default();
                meta.add_error("some error", None);
                meta
            }),
            culprit: Annotated::empty(),
            transaction: Annotated::empty(),
            message: Annotated::empty(),
            logentry: Annotated::empty(),
            logger: Annotated::empty(),
            modules: Annotated::empty(),
            platform: Annotated(Some("other".to_string()), {
                let mut meta = Meta::default();
                meta.add_error("some error", None);
                meta
            }),
            timestamp: Annotated::empty(),
            server_name: Annotated::empty(),
            release: Annotated::empty(),
            dist: Annotated::empty(),
            environment: Annotated::empty(),
            user: Annotated::empty(),
            request: Annotated::empty(),
            //contexts: Default::default(), TODO
            breadcrumbs: Annotated::empty(),
            exceptions: Annotated::empty(),
            stacktrace: Annotated::empty(),
            template_info: Annotated::empty(),
            threads: Annotated::empty(),
            tags: Annotated::empty(),
            geo: Annotated::empty(),
            extra: Annotated::empty(),
            debug_meta: Annotated::empty(),
            client_sdk: Annotated::empty(),
            other: Default::default(),
        });

        assert_eq_dbg!(event, Annotated::<Event>::from_json(json).unwrap());
        assert_eq_str!(json, event.to_json_pretty().unwrap());
    }
}
