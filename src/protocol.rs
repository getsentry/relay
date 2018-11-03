use chrono::{DateTime, Utc};
use cookie::Cookie;
use debugid::DebugId;
use serde::{Serialize, Serializer};
use serde_json;
use url::form_urlencoded;
use uuid::Uuid;

use general_derive::{FromValue, ProcessValue, ToValue};

use crate::meta::{Annotated, Value};
use crate::processor::{FromValue, ProcessValue, ToValue};
use crate::types::{
    Addr, Array, EventId, LenientString, Level, Map, Object, RegVal, ThreadId, Values,
};

#[cfg(test)]
use chrono::TimeZone;
#[cfg(test)]
use crate::meta::Meta;

#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    /// Unique identifier of this event.
    #[metastructure(field = "event_id")]
    pub id: Annotated<EventId>,

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
    #[metastructure(legacy_alias = "sentry.interfaces.Contexts")]
    pub contexts: Annotated<Contexts>,

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
    pub tags: Annotated<Tags>,

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
            // TODO: check error reporting here, this seems wrong
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

                            meta.add_unexpected_value_error(
                                "number, string or bool",
                                value_opt.unwrap_or(Value::Null),
                            );
                            return Annotated(None, meta);
                        }
                    }
                }
                Annotated(Some(Fingerprint(fingerprint)), meta)
            }
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("array", value);
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Headers(pub Map<String, String>);

fn normalize_header(key: &str) -> String {
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
        })
}

impl FromValue for Headers {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type HeaderTuple = (Annotated<String>, Annotated<String>);
        match value {
            Annotated(Some(Value::Array(items)), mut meta) => {
                let mut rv = Map::new();
                let mut bad_items = vec![];
                for item in items.into_iter() {
                    match HeaderTuple::from_value(item) {
                        // simple case: valid key.  In that case we take the value as such and
                        // merge it with the tuple level metadata.
                        Annotated(
                            Some((Annotated(Some(key), _), Annotated(value, value_meta))),
                            pair_meta,
                        ) => {
                            rv.insert(
                                normalize_header(&key),
                                Annotated(value, pair_meta.merge(value_meta)),
                            );
                        }
                        // complex case: we didn't get a key out for one reason or another
                        // which means we cannot create a entry in the hashmap.
                        Annotated(
                            Some((Annotated(None, mut key_meta), value @ Annotated(..))),
                            _,
                        ) => {
                            let mut value = ToValue::to_value(value);
                            let key = key_meta.take_original_value();
                            let value = value.0.take().or_else(|| value.1.take_original_value());
                            if let (Some(key), Some(value)) = (key, value) {
                                bad_items.push(Annotated::new(Value::Array(vec![
                                    Annotated::new(key),
                                    Annotated::new(value),
                                ])));
                            }
                        }
                        Annotated(_, mut pair_meta) => {
                            if let Some(value) = pair_meta.take_original_value() {
                                bad_items.push(Annotated::new(value));
                            }
                        }
                    }
                }
                if !bad_items.is_empty() {
                    meta.add_error(
                        "invalid non-header values",
                        if bad_items.is_empty() {
                            None
                        } else {
                            Some(Value::Array(bad_items))
                        },
                    );
                }
                Annotated(Some(Headers(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(Headers(
                    items
                        .into_iter()
                        .map(|(key, value)| (normalize_header(&key), String::from_value(value)))
                        .collect(),
                )),
                meta,
            ),
            other => FromValue::from_value(other).map_value(Headers),
        }
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
                            | v @ Annotated(Some(Value::Null), _) => (k, FromValue::from_value(v)),
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
                                (k, FromValue::from_value(v))
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

/// Tags
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Tags(pub Array<(Annotated<String>, Annotated<String>)>);

impl FromValue for Tags {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type TagTuple = (Annotated<LenientString>, Annotated<LenientString>);
        match value {
            Annotated(Some(Value::Array(items)), meta) => {
                let mut rv = Vec::new();
                for item in items.into_iter() {
                    rv.push(TagTuple::from_value(item).map_value(|tuple| {
                        (
                            tuple.0.map_value(|key| key.0.trim().replace(" ", "-")),
                            tuple.1.map_value(|v| v.0),
                        )
                    }));
                }
                Annotated(Some(Tags(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => {
                let mut rv = Vec::new();
                for (key, value) in items.into_iter() {
                    rv.push((
                        key.trim().replace(" ", "-"),
                        LenientString::from_value(value),
                    ));
                }
                rv.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                Annotated(
                    Some(Tags(
                        rv.into_iter()
                            .map(|(k, v)| Annotated::new((Annotated::new(k), v.map_value(|x| x.0))))
                            .collect(),
                    )),
                    meta,
                )
            }
            other => FromValue::from_value(other).map_value(Tags),
        }
    }
}

/// Http request information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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

#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_stacktrace")]
pub struct Stacktrace {
    #[metastructure(required = "true")]
    pub frames: Annotated<Array<Frame>>,

    /// Register values of the thread (top frame).
    pub registers: Annotated<Object<RegVal>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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

#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct ClientSdkInfo {
    /// Unique SDK name.
    #[metastructure(required = "true")]
    pub name: Annotated<String>,

    /// SDK version.
    #[metastructure(required = "true")]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct ClientSdkPackage {
    /// Name of the package.
    pub name: Annotated<String>,
    /// Version of the package.
    pub version: Annotated<String>,
}

/// Debugging and processing meta information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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

/// A debug information file (debug image).
#[derive(Debug, Clone, PartialEq, ToValue, FromValue, ProcessValue)]
pub enum DebugImage {
    /// Apple debug images (machos).  This is currently also used for non apple platforms with
    /// similar debug setups.
    Apple(Box<AppleDebugImage>),
    /// Symbolic (new style) debug infos.
    Symbolic(Box<SymbolicDebugImage>),
    /// A reference to a proguard debug file.
    Proguard(Box<ProguardDebugImage>),
    /// A debug image that is unknown to this protocol specification.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

/// Apple debug image in
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct AppleDebugImage {
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct SymbolicDebugImage {
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct ProguardDebugImage {
    /// UUID computed from the file contents.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Geographical location of the end user or device.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
pub enum Context {
    /// Device information.
    Device(Box<DeviceContext>),
    /// Operating system information.
    Os(Box<OsContext>),
    /// Runtime information.
    Runtime(Box<RuntimeContext>),
    /// Application information.
    App(Box<AppContext>),
    /// Web browser information.
    Browser(Box<BrowserContext>),
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Contexts(pub Object<Context>);

impl FromValue for Contexts {
    fn from_value(mut annotated: Annotated<Value>) -> Annotated<Self> {
        if let Annotated(Some(Value::Object(ref mut items)), _) = annotated {
            for (key, value) in items.iter_mut() {
                if let Annotated(Some(Value::Object(ref mut items)), _) = value {
                    if !items.contains_key("type") {
                        items.insert(
                            "type".to_string(),
                            Annotated::new(Value::String(key.to_string())),
                        );
                    }
                }
            }
        }
        FromValue::from_value(annotated).map_value(Contexts)
    }
}

/// Device information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct Breadcrumb {
    /// The timestamp of the breadcrumb (required).
    #[metastructure(required = "true")]
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
#[derive(Debug, Clone, PartialEq, Default, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct CError {
    /// The error code as specified by ISO C99, POSIX.1-2001 or POSIX.1-2008.
    #[metastructure(required = "true")]
    pub number: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,
}

/// Mach exception information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
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
  "uuid": "395835f4-03e0-4436-80d3-136f0749a893",
  "other": "value",
  "type": "proguard"
}"#;
    let image = Annotated::new(DebugImage::Proguard(Box::new(ProguardDebugImage {
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
  "name": "CoreFoundation",
  "arch": "arm64",
  "cpu_type": 1233,
  "cpu_subtype": 3,
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "uuid": "494f3aea-88fa-4296-9644-fa8ef5d139b6",
  "other": "value",
  "type": "apple"
}"#;

    let image = Annotated::new(DebugImage::Apple(Box::new(AppleDebugImage {
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
  "name": "CoreFoundation",
  "image_addr": "0x0",
  "image_size": 4096,
  "uuid": "494f3aea-88fa-4296-9644-fa8ef5d139b6",
  "type": "apple"
}"#;

    let image = Annotated::new(DebugImage::Apple(Box::new(AppleDebugImage {
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
  "name": "CoreFoundation",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234",
  "other": "value",
  "type": "symbolic"
}"#;

    let image = Annotated::new(DebugImage::Symbolic(Box::new(SymbolicDebugImage {
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
  "name": "CoreFoundation",
  "image_addr": "0x0",
  "image_size": 4096,
  "id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234",
  "type": "symbolic"
}"#;

    let image = Annotated::new(DebugImage::Symbolic(Box::new(SymbolicDebugImage {
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
                "Referer".to_string(),
                Annotated::new("https://google.com/".to_string()),
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
  "message": "mymessage",
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
        contexts: Annotated::empty(),
        breadcrumbs: Annotated::empty(),
        exceptions: Annotated::empty(),
        stacktrace: Annotated::empty(),
        template_info: Annotated::empty(),
        threads: Annotated::empty(),
        tags: {
            let mut items = Array::new();
            items.push(Annotated::new((
                Annotated::new("tag".to_string()),
                Annotated::new("value".to_string()),
            )));
            Annotated::new(Tags(items))
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
fn test_event_default_values() {
    let json = "{}";
    let event = Annotated::new(Event {
        id: Annotated::empty(),
        level: Annotated::empty(),
        fingerprint: Annotated::empty(),
        culprit: Annotated::empty(),
        transaction: Annotated::empty(),
        message: Annotated::empty(),
        logentry: Annotated::empty(),
        logger: Annotated::empty(),
        modules: Annotated::empty(),
        platform: Annotated::empty(),
        timestamp: Annotated::empty(),
        server_name: Annotated::empty(),
        release: Annotated::empty(),
        dist: Annotated::empty(),
        environment: Annotated::empty(),
        user: Annotated::empty(),
        request: Annotated::empty(),
        contexts: Annotated::empty(),
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
        contexts: Annotated::empty(),
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

#[test]
fn test_fingerprint_string() {
    assert_eq_dbg!(
        Annotated::new(vec!["fingerprint".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[\"fingerprint\"]").unwrap()
    );
}

#[test]
fn test_fingerprint_bool() {
    assert_eq_dbg!(
        Annotated::new(vec!["True".to_string(), "False".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[true, false]").unwrap()
    );
}

#[test]
fn test_fingerprint_number() {
    assert_eq_dbg!(
        Annotated::new(vec!["-22".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[-22]").unwrap()
    );
}

#[test]
fn test_fingerprint_float() {
    assert_eq_dbg!(
        Annotated::new(vec!["3".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[3.0]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_trunc() {
    assert_eq_dbg!(
        Annotated::new(vec!["3".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[3.5]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_strip() {
    assert_eq_dbg!(
        Annotated::new(vec![].into()),
        Annotated::<Fingerprint>::from_json("[-1e100]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_bounds() {
    assert_eq_dbg!(
        Annotated::new(vec![].into()),
        Annotated::<Fingerprint>::from_json("[1.7976931348623157e+308]").unwrap()
    );
}

#[test]
fn test_fingerprint_invalid_fallback() {
    assert_eq_dbg!(
        Annotated(None, {
            let mut meta = Meta::default();
            meta.add_error("expected number, string or bool", None);
            meta
        }),
        Annotated::<Fingerprint>::from_json("[\"a\", null, \"d\"]").unwrap()
    );
}

#[test]
fn test_fingerprint_empty() {
    assert_eq_dbg!(
        Annotated::new(vec![].into()),
        Annotated::<Fingerprint>::from_json("[]").unwrap()
    );
}

#[test]
fn test_client_sdk_roundtrip() {
    let json = r#"{
  "name": "sentry.rust",
  "version": "1.0.0",
  "integrations": [
    "actix"
  ],
  "packages": [
    {
      "name": "cargo:sentry",
      "version": "0.10.0"
    },
    {
      "name": "cargo:sentry-actix",
      "version": "0.10.0"
    }
  ],
  "other": "value"
}"#;
    let sdk = Annotated::new(ClientSdkInfo {
        name: Annotated::new("sentry.rust".to_string()),
        version: Annotated::new("1.0.0".to_string()),
        integrations: Annotated::new(vec![Annotated::new("actix".to_string())]),
        packages: Annotated::new(vec![
            Annotated::new(ClientSdkPackage {
                name: Annotated::new("cargo:sentry".to_string()),
                version: Annotated::new("0.10.0".to_string()),
            }),
            Annotated::new(ClientSdkPackage {
                name: Annotated::new("cargo:sentry-actix".to_string()),
                version: Annotated::new("0.10.0".to_string()),
            }),
        ]),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(sdk, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, sdk.to_json_pretty().unwrap());
}

#[test]
fn test_client_sdk_default_values() {
    let json = r#"{
  "name": "sentry.rust",
  "version": "1.0.0"
}"#;
    let sdk = Annotated::new(ClientSdkInfo {
        name: Annotated::new("sentry.rust".to_string()),
        version: Annotated::new("1.0.0".to_string()),
        integrations: Annotated::empty(),
        packages: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(sdk, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, sdk.to_json_pretty().unwrap());
}

#[test]
fn test_client_sdk_invalid() {
    let json = r#"{"name":"sentry.rust"}"#;
    let entry = Annotated::new(ClientSdkInfo {
        name: Annotated::new("sentry.rust".to_string()),
        version: Annotated::from_error("value required", None),
        integrations: Annotated::empty(),
        packages: Annotated::empty(),
        other: Default::default(),
    });
    assert_eq_dbg!(entry, Annotated::from_json(json).unwrap());
}

#[test]
fn test_debug_meta_roundtrip() {
    // NOTE: images are tested separately
    let json = r#"{
  "sdk_info": {
    "sdk_name": "iOS",
    "version_major": 10,
    "version_minor": 3,
    "version_patchlevel": 0,
    "other": "value"
  },
  "other": "value"
}"#;
    let meta = Annotated::new(DebugMeta {
        system_sdk: Annotated::new(SystemSdkInfo {
            sdk_name: Annotated::new("iOS".to_string()),
            version_major: Annotated::new(10),
            version_minor: Annotated::new(3),
            version_patchlevel: Annotated::new(0),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        }),
        images: Annotated::empty(),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(meta, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, meta.to_json_pretty().unwrap());
}

#[test]
fn test_debug_meta_default_values() {
    let json = "{}";
    let meta = Annotated::new(DebugMeta {
        system_sdk: Annotated::empty(),
        images: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(meta, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, meta.to_json_pretty().unwrap());
}

#[test]
fn test_geo_roundtrip() {
    let json = r#"{
  "country_code": "US",
  "city": "San Francisco",
  "region": "CA",
  "other": "value"
}"#;
    let geo = Annotated::new(Geo {
        country_code: Annotated::new("US".to_string()),
        city: Annotated::new("San Francisco".to_string()),
        region: Annotated::new("CA".to_string()),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(geo, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, geo.to_json_pretty().unwrap());
}

#[test]
fn test_geo_default_values() {
    let json = "{}";
    let geo = Annotated::new(Geo {
        country_code: Annotated::empty(),
        city: Annotated::empty(),
        region: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(geo, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, geo.to_json_pretty().unwrap());
}

#[test]
fn test_thread_roundtrip() {
    // stack traces are tested separately
    let json = r#"{
  "id": 42,
  "name": "myname",
  "crashed": true,
  "current": true,
  "other": "value"
}"#;
    let thread = Annotated::new(Thread {
        id: Annotated::new(ThreadId::Int(42)),
        name: Annotated::new("myname".to_string()),
        stacktrace: Annotated::empty(),
        raw_stacktrace: Annotated::empty(),
        crashed: Annotated::new(true),
        current: Annotated::new(true),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(thread, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, thread.to_json_pretty().unwrap());
}

#[test]
fn test_thread_default_values() {
    let json = "{}";
    let thread = Annotated::new(Thread {
        id: Annotated::empty(),
        name: Annotated::empty(),
        stacktrace: Annotated::empty(),
        raw_stacktrace: Annotated::empty(),
        crashed: Annotated::empty(),
        current: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(thread, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, thread.to_json_pretty().unwrap());
}

#[test]
fn test_exception_roundtrip() {
    // stack traces and mechanism are tested separately
    let json = r#"{
  "type": "mytype",
  "value": "myvalue",
  "module": "mymodule",
  "thread_id": 42,
  "other": "value"
}"#;
    let exception = Annotated::new(Exception {
        ty: Annotated::new("mytype".to_string()),
        value: Annotated::new("myvalue".to_string()),
        module: Annotated::new("mymodule".to_string()),
        stacktrace: Annotated::empty(),
        raw_stacktrace: Annotated::empty(),
        thread_id: Annotated::new(ThreadId::Int(42)),
        mechanism: Annotated::empty(),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(exception, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, exception.to_json_pretty().unwrap());
}

#[test]
fn test_exception_default_values() {
    let json = r#"{"type":"mytype"}"#;
    let exception = Annotated::new(Exception {
        ty: Annotated::new("mytype".to_string()),
        value: Annotated::empty(),
        module: Annotated::empty(),
        stacktrace: Annotated::empty(),
        raw_stacktrace: Annotated::empty(),
        thread_id: Annotated::empty(),
        mechanism: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(exception, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, exception.to_json().unwrap());
}

#[test]
fn test_exception_without_type() {
    assert_eq!(
        false,
        Annotated::<Exception>::from_json("{}")
            .unwrap()
            .1
            .has_errors()
    );
}

#[test]
fn test_exception_invalid() {
    let exception = Annotated::new(Exception {
        ty: Annotated::from_error("value required", None),
        value: Annotated::empty(),
        module: Annotated::empty(),
        stacktrace: Annotated::empty(),
        raw_stacktrace: Annotated::empty(),
        thread_id: Annotated::empty(),
        mechanism: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(exception, Annotated::from_json("{}").unwrap());
}

#[test]
fn test_breadcrumb_roundtrip() {
    let input = r#"{
  "timestamp": 946684800,
  "type": "mytype",
  "category": "mycategory",
  "level": "fatal",
  "message": "my message",
  "data": {
    "a": "b"
  },
  "c": "d"
}"#;

    let output = r#"{
  "timestamp": 946684800.0,
  "type": "mytype",
  "category": "mycategory",
  "level": "fatal",
  "message": "my message",
  "data": {
    "a": "b"
  },
  "c": "d"
}"#;

    let breadcrumb = Annotated::new(Breadcrumb {
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
        ty: Annotated::new("mytype".to_string()),
        category: Annotated::new("mycategory".to_string()),
        level: Annotated::new(Level::Fatal),
        message: Annotated::new("my message".to_string()),
        data: {
            let mut map = Map::new();
            map.insert(
                "a".to_string(),
                Annotated::new(Value::String("b".to_string())),
            );
            Annotated::new(map)
        },
        other: {
            let mut map = Map::new();
            map.insert(
                "c".to_string(),
                Annotated::new(Value::String("d".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(breadcrumb, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, breadcrumb.to_json_pretty().unwrap());
}

#[test]
fn test_breadcrumb_default_values() {
    let input = r#"{"timestamp":946684800}"#;
    let output = r#"{"timestamp":946684800.0}"#;

    let breadcrumb = Annotated::new(Breadcrumb {
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
        ty: Annotated::empty(),
        category: Annotated::empty(),
        level: Annotated::empty(),
        message: Annotated::empty(),
        data: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(breadcrumb, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, breadcrumb.to_json().unwrap());
}

#[test]
fn test_breadcrumb_invalid() {
    let breadcrumb = Annotated::new(Breadcrumb {
        timestamp: Annotated::from_error("value required", None),
        ty: Annotated::empty(),
        category: Annotated::empty(),
        level: Annotated::empty(),
        message: Annotated::empty(),
        data: Annotated::empty(),
        other: Default::default(),
    });
    assert_eq_dbg!(breadcrumb, Annotated::from_json("{}").unwrap());
}

#[test]
fn test_frame_roundtrip() {
    let json = r#"{
  "function": "main",
  "symbol": "_main",
  "module": "app",
  "package": "/my/app",
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
  "in_app": true,
  "vars": {
    "variable": "value"
  },
  "image_addr": "0x400",
  "instruction_addr": "0x404",
  "symbol_addr": "0x404",
  "other": "value"
}"#;
    let frame = Annotated::new(Frame {
        function: Annotated::new("main".to_string()),
        symbol: Annotated::new("_main".to_string()),
        module: Annotated::new("app".to_string()),
        package: Annotated::new("/my/app".to_string()),
        filename: Annotated::new("myfile.rs".to_string()),
        abs_path: Annotated::new("/path/to".to_string()),
        line: Annotated::new(2),
        column: Annotated::new(42),
        pre_lines: Annotated::new(vec![Annotated::new("fn main() {".to_string())]),
        current_line: Annotated::new("unimplemented!()".to_string()),
        post_lines: Annotated::new(vec![Annotated::new("}".to_string())]),
        in_app: Annotated::new(true),
        vars: {
            let mut map = Map::new();
            map.insert(
                "variable".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            Annotated::new(map)
        },
        image_addr: Annotated::new(Addr(0x400)),
        instruction_addr: Annotated::new(Addr(0x404)),
        symbol_addr: Annotated::new(Addr(0x404)),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(frame, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, frame.to_json_pretty().unwrap());
}

#[test]
fn test_frame_default_values() {
    let json = "{}";
    let frame = Annotated::new(Frame {
        function: Annotated::empty(),
        symbol: Annotated::empty(),
        module: Annotated::empty(),
        package: Annotated::empty(),
        filename: Annotated::empty(),
        abs_path: Annotated::empty(),
        line: Annotated::empty(),
        column: Annotated::empty(),
        pre_lines: Annotated::empty(),
        current_line: Annotated::empty(),
        post_lines: Annotated::empty(),
        in_app: Annotated::empty(),
        vars: Annotated::empty(),
        image_addr: Annotated::empty(),
        instruction_addr: Annotated::empty(),
        symbol_addr: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(frame, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, frame.to_json_pretty().unwrap());
}

#[test]
fn test_stacktrace_roundtrip() {
    let json = r#"{
  "frames": [],
  "registers": {
    "cspr": "0x20000000",
    "lr": "0x18a31aadc",
    "pc": "0x18a310ea4",
    "sp": "0x16fd75060"
  },
  "other": "value"
}"#;
    let stack = Annotated::new(Stacktrace {
        frames: Annotated::new(Default::default()),
        registers: {
            let mut map = Map::new();
            map.insert("cspr".to_string(), Annotated::new(RegVal(0x2000_0000)));
            map.insert("lr".to_string(), Annotated::new(RegVal(0x1_8a31_aadc)));
            map.insert("pc".to_string(), Annotated::new(RegVal(0x1_8a31_0ea4)));
            map.insert("sp".to_string(), Annotated::new(RegVal(0x1_6fd7_5060)));
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
    });

    assert_eq_dbg!(stack, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, stack.to_json_pretty().unwrap());
}

#[test]
fn test_stacktrace_default_values() {
    let json = r#"{"frames":[]}"#;
    let stack = Annotated::new(Stacktrace {
        frames: Annotated::new(Default::default()),
        registers: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(stack, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, stack.to_json().unwrap());
}

#[test]
fn test_stacktrace_invalid() {
    let stack = Annotated::new(Stacktrace {
        frames: Annotated::from_error("value required", None),
        registers: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(stack, Annotated::from_json("{}").unwrap());
}

#[test]
fn test_device_context_roundtrip() {
    let json = r#"{
  "name": "iphone",
  "family": "iphone",
  "model": "iphone7,3",
  "model_id": "AH223",
  "arch": "arm64",
  "battery_level": 58.5,
  "orientation": "landscape",
  "simulator": true,
  "memory_size": 3137978368,
  "free_memory": 322781184,
  "usable_memory": 2843525120,
  "storage_size": 63989469184,
  "free_storage": 31994734592,
  "external_storage_size": 2097152,
  "external_free_storage": 2097152,
  "boot_time": 1518094332.0,
  "timezone": "Europe/Vienna",
  "other": "value",
  "type": "device"
}"#;
    let context = Annotated::new(Context::Device(Box::new(DeviceContext {
        name: Annotated::new("iphone".to_string()),
        family: Annotated::new("iphone".to_string()),
        model: Annotated::new("iphone7,3".to_string()),
        model_id: Annotated::new("AH223".to_string()),
        arch: Annotated::new("arm64".to_string()),
        battery_level: Annotated::new(58.5),
        orientation: Annotated::new("landscape".to_string()),
        simulator: Annotated::new(true),
        memory_size: Annotated::new(3_137_978_368),
        free_memory: Annotated::new(322_781_184),
        usable_memory: Annotated::new(2_843_525_120),
        storage_size: Annotated::new(63_989_469_184),
        free_storage: Annotated::new(31_994_734_592),
        external_storage_size: Annotated::new(2_097_152),
        external_free_storage: Annotated::new(2_097_152),
        boot_time: Annotated::new("2018-02-08T12:52:12Z".parse().unwrap()),
        timezone: Annotated::new("Europe/Vienna".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_os_context_roundtrip() {
    let json = r#"{
  "name": "iOS",
  "version": "11.4.2",
  "build": "FEEDFACE",
  "kernel_version": "17.4.0",
  "rooted": true,
  "raw_description": "iOS 11.4.2 FEEDFACE (17.4.0)",
  "other": "value",
  "type": "os"
}"#;
    let context = Annotated::new(Context::Os(Box::new(OsContext {
        name: Annotated::new("iOS".to_string()),
        version: Annotated::new("11.4.2".to_string()),
        build: Annotated::new("FEEDFACE".to_string()),
        kernel_version: Annotated::new("17.4.0".to_string()),
        rooted: Annotated::new(true),
        raw_description: Annotated::new("iOS 11.4.2 FEEDFACE (17.4.0)".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_runtime_context_roundtrip() {
    let json = r#"{
  "name": "rustc",
  "version": "1.27.0",
  "raw_description": "rustc 1.27.0 stable",
  "other": "value",
  "type": "runtime"
}"#;
    let context = Annotated::new(Context::Runtime(Box::new(RuntimeContext {
        name: Annotated::new("rustc".to_string()),
        version: Annotated::new("1.27.0".to_string()),
        raw_description: Annotated::new("rustc 1.27.0 stable".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_app_context_roundtrip() {
    let json = r#"{
  "app_start_time": 1518128517.0,
  "device_app_hash": "4c793e3776474877ae30618378e9662a",
  "build_type": "testflight",
  "app_identifier": "foo.bar.baz",
  "app_name": "Baz App",
  "app_version": "1.0",
  "app_build": "100001",
  "other": "value",
  "type": "app"
}"#;
    let context = Annotated::new(Context::App(Box::new(AppContext {
        app_start_time: Annotated::new("2018-02-08T22:21:57Z".parse().unwrap()),
        device_app_hash: Annotated::new("4c793e3776474877ae30618378e9662a".to_string()),
        build_type: Annotated::new("testflight".to_string()),
        app_identifier: Annotated::new("foo.bar.baz".to_string()),
        app_name: Annotated::new("Baz App".to_string()),
        app_version: Annotated::new("1.0".to_string()),
        app_build: Annotated::new("100001".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_browser_context_roundtrip() {
    let json = r#"{
  "name": "Google Chrome",
  "version": "67.0.3396.99",
  "other": "value",
  "type": "browser"
}"#;
    let context = Annotated::new(Context::Browser(Box::new(BrowserContext {
        name: Annotated::new("Google Chrome".to_string()),
        version: Annotated::new("67.0.3396.99".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_other_context_roundtrip() {
    let json = r#"{"other":"value","type":"mytype"}"#;
    let context = Annotated::new(Context::Other({
        let mut map = Map::new();
        map.insert(
            "other".to_string(),
            Annotated::new(Value::String("value".to_string())),
        );
        map.insert(
            "type".to_string(),
            Annotated::new(Value::String("mytype".to_string())),
        );
        map
    }));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json().unwrap());
}

#[test]
fn test_untagged_context_deserialize() {
    let json = r#"{"os": {"name": "Linux"}}"#;

    let os_context = Annotated::new(Context::Os(Box::new(OsContext {
        name: Annotated::new("Linux".to_string()),
        ..Default::default()
    })));
    let mut map = Object::new();
    map.insert("os".to_string(), os_context);
    let contexts = Annotated::new(Contexts(map));

    assert_eq_dbg!(contexts, Annotated::from_json(json).unwrap());
}

#[test]
fn test_header_normalization() {
    let json = r#"{
  "-other-": "header",
  "accept": "application/json",
  "x-sentry": "version=8"
}"#;

    let mut map = Map::new();
    map.insert(
        "Accept".to_string(),
        Annotated::new("application/json".to_string()),
    );
    map.insert(
        "X-Sentry".to_string(),
        Annotated::new("version=8".to_string()),
    );
    map.insert("-Other-".to_string(), Annotated::new("header".to_string()));

    let headers = Annotated::new(Headers(map));
    assert_eq_dbg!(headers, Annotated::from_json(json).unwrap());
}

#[test]
fn test_header_from_sequence() {
    let json = r#"[
  ["accept", "application/json"]
]"#;

    let mut map = Map::new();
    map.insert(
        "Accept".to_string(),
        Annotated::new("application/json".to_string()),
    );

    let headers = Annotated::new(Headers(map));
    assert_eq_dbg!(headers, Annotated::from_json(json).unwrap());

    let json = r#"[
  ["accept", "application/json"],
  ["whatever", 42],
  [1, 2],
  ["a", "b", "c"],
  23
]"#;
    let headers = Annotated::<Headers>::from_json(json).unwrap();
    assert_eq_str!(headers.to_json().unwrap(), r#"{"Accept":"application/json","Whatever":null,"_meta":{"":{"err":["invalid non-header values"],"val":[[1,2],["a","b","c"],23]},"Whatever":{"":{"err":["expected a string"],"val":42}}}}"#);
}

#[test]
fn test_tags_from_object() {
    let json = r#"{
  "blah": "blub",
  "bool": true,
  "foo bar": "baz",
  "non string": 42
}"#;

    let mut arr = Array::new();
    arr.push(Annotated::new((
        Annotated::new("blah".to_string()),
        Annotated::new("blub".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("bool".to_string()),
        Annotated::new("True".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("foo-bar".to_string()),
        Annotated::new("baz".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("non-string".to_string()),
        Annotated::new("42".to_string()),
    )));

    let tags = Annotated::new(Tags(arr));
    assert_eq_dbg!(tags, Annotated::from_json(json).unwrap());
}

#[test]
fn test_tags_from_array() {
    let json = r#"[
  ["bool", true],
  ["foo bar", "baz"],
  [23, 42],
  ["blah", "blub"]
]"#;

    let mut arr = Array::new();
    arr.push(Annotated::new((
        Annotated::new("bool".to_string()),
        Annotated::new("True".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("foo-bar".to_string()),
        Annotated::new("baz".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("23".to_string()),
        Annotated::new("42".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("blah".to_string()),
        Annotated::new("blub".to_string()),
    )));

    let tags = Annotated::new(Tags(arr));
    assert_eq_dbg!(tags, Annotated::from_json(json).unwrap());
}
