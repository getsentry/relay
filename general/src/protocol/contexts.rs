use std::fmt;
use std::str::FromStr;

use failure::Fail;
use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{Annotated, Empty, Error, FromValue, Object, SkipSerialization, ToValue, Value};

/// Device information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
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

    /// Manufacturer of the device
    pub manufacturer: Annotated<String>,

    /// Brand of the device.
    pub brand: Annotated<String>,

    /// Device screen resolution.
    pub screen_resolution: Annotated<String>,

    /// Device screen density.
    pub screen_density: Annotated<f64>,

    /// Screen density as dots-per-inch.
    pub screen_dpi: Annotated<u64>,

    /// Whether the device was online or not.
    pub online: Annotated<bool>,

    /// Whether the device was charging or not.
    pub charging: Annotated<bool>,

    /// Whether the device was low on memory.
    pub low_memory: Annotated<bool>,

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
    pub boot_time: Annotated<String>,

    /// Timezone of the device.
    pub timezone: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl DeviceContext {
    /// The key under which a device context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "device"
    }
}

/// Operating system information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct OsContext {
    /// Name of the operating system.
    pub name: Annotated<String>,

    /// Version of the operating system.
    pub version: Annotated<String>,

    /// Internal build number of the operating system.
    pub build: Annotated<LenientString>,

    /// Current kernel version.
    pub kernel_version: Annotated<String>,

    /// Indicator if the OS is rooted (mobile mostly).
    pub rooted: Annotated<bool>,

    /// Unprocessed operating system info.
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl OsContext {
    /// The key under which an os context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "os"
    }
}

/// Runtime information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct RuntimeContext {
    /// Runtime name.
    pub name: Annotated<String>,

    /// Runtime version string.
    pub version: Annotated<String>,

    /// Application build string, if it is separate from the version.
    pub build: Annotated<LenientString>,

    /// Unprocessed runtime info.
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl RuntimeContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "runtime"
    }
}

/// Application information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct AppContext {
    /// Start time of the app.
    pub app_start_time: Annotated<String>,

    /// Device app hash (app specific device ID)
    pub device_app_hash: Annotated<String>,

    /// Build identicator.
    pub build_type: Annotated<String>,

    /// App identifier (dotted bundle id).
    pub app_identifier: Annotated<String>,

    /// Application name as it appears on the platform.
    pub app_name: Annotated<String>,

    /// Application version as it appears on the platform.
    pub app_version: Annotated<String>,

    /// Internal build ID as it appears on the platform.
    pub app_build: Annotated<LenientString>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl AppContext {
    /// The key under which an app context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "app"
    }
}

/// Web browser information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct BrowserContext {
    /// Runtime name.
    pub name: Annotated<String>,

    /// Runtime version.
    pub version: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl BrowserContext {
    /// The key under which a browser context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "browser"
    }
}

/// Operation type such as `db.statement` for database queries or `http` for external HTTP calls.
/// Tries to follow OpenCensus/OpenTracing's span types.
pub type OperationType = String;

lazy_static::lazy_static! {
    static ref TRACE_ID: Regex = Regex::new("^[a-fA-F0-9]{32}$").unwrap();
    static ref SPAN_ID: Regex = Regex::new("^[a-fA-F0-9]{16}$").unwrap();
}

/// GPU information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct GpuContext(pub Object<Value>);

impl From<Object<Value>> for GpuContext {
    fn from(object: Object<Value>) -> Self {
        Self(object)
    }
}

impl std::ops::Deref for GpuContext {
    type Target = Object<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for GpuContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl GpuContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "gpu"
    }
}

/// Monitor information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct MonitorContext(pub Object<Value>);

impl From<Object<Value>> for MonitorContext {
    fn from(object: Object<Value>) -> Self {
        Self(object)
    }
}

impl std::ops::Deref for MonitorContext {
    type Target = Object<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MonitorContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl MonitorContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "monitor"
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
pub struct TraceId(pub String);

impl FromValue for TraceId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                if !TRACE_ID.is_match(&value) || value.bytes().all(|x| x == b'0') {
                    meta.add_error(Error::invalid("not a valid trace id"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                } else {
                    Annotated(Some(TraceId(value.to_ascii_lowercase())), meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("trace id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
pub struct SpanId(pub String);

impl FromValue for SpanId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                if !SPAN_ID.is_match(&value) || value.bytes().all(|x| x == b'0') {
                    meta.add_error(Error::invalid("not a valid span id"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                } else {
                    Annotated(Some(SpanId(value.to_ascii_lowercase())), meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("span id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// Trace context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct TraceContext {
    /// The trace ID.
    pub trace_id: Annotated<TraceId>,

    /// The ID of the span.
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = "enumlike")]
    pub op: Annotated<OperationType>,

    /// Whether the trace failed or succeeded. Currently only used to indicate status of individual
    /// transactions.
    pub status: Annotated<SpanStatus>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

/// Trace status
///
/// Values from https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/api-tracing.md#status
/// Mapping to HTTP from https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/data-http.md#status
///
/// Note: This type is represented as a u8 in Snuba/Clickhouse, with Unknown being the default
/// value. We use repr(u8) to statically validate that the trace status has 255 variants at most.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)] // size limit in clickhouse
pub enum SpanStatus {
    // XXX: this mapping exists multiple times at the moment.  It's also in the python binding
    // to semaphore.  This should be cleaned up.
    /// The operation completed successfully.
    ///
    /// HTTP status 100..299 + successful redirects from the 3xx range.
    Ok = 0,

    /// The operation was cancelled (typically by the user).
    Cancelled = 1,

    /// Unknown. Any non-standard HTTP status code.
    ///
    /// "We do not know whether the transaction failed or succeeded"
    UnknownError = 2,

    /// Client specified an invalid argument. 4xx.
    ///
    /// Note that this differs from FailedPrecondition. InvalidArgument indicates arguments that
    /// are problematic regardless of the state of the system.
    InvalidArgument = 3,

    /// Deadline expired before operation could complete.
    ///
    /// For operations that change the state of the system, this error may be returned even if the
    /// operation has been completed successfully.
    ///
    /// HTTP redirect loops and 504 Gateway Timeout
    DeadlineExceeded = 4,

    /// 404 Not Found. Some requested entity (file or directory) was not found.
    NotFound = 5,

    /// Already exists (409)
    ///
    /// Some entity that we attempted to create already exists.
    AlreadyExists = 6,

    /// 403 Forbidden
    ///
    /// The caller does not have permission to execute the specified operation.
    PermissionDenied = 7,

    /// 429 Too Many Requests
    ///
    /// Some resource has been exhausted, perhaps a per-user quota or perhaps the entire file
    /// system is out of space.
    ResourceExhausted = 8,

    /// Operation was rejected because the system is not in a state required for the operation's
    /// execution
    FailedPrecondition = 9,

    /// The operation was aborted, typically due to a concurrency issue.
    Aborted = 10,

    /// Operation was attempted past the valid range.
    OutOfRange = 11,

    /// 501 Not Implemented
    ///
    /// Operation is not implemented or not enabled.
    Unimplemented = 12,

    /// Other/generic 5xx.
    InternalError = 13,

    /// 503 Service Unavailable
    Unavailable = 14,

    /// Unrecoverable data loss or corruption
    DataLoss = 15,

    /// 401 Unauthorized (actually does mean unauthenticated according to RFC 7235)
    ///
    /// Prefer PermissionDenied if a user is logged in.
    Unauthenticated = 16,
}

impl ProcessValue for SpanStatus {}

impl Empty for SpanStatus {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Debug, Fail)]
#[fail(display = "invalid span status")]
pub struct ParseSpanStatusError;

impl FromStr for SpanStatus {
    type Err = ParseSpanStatusError;

    fn from_str(string: &str) -> Result<SpanStatus, Self::Err> {
        Ok(match string {
            "ok" => SpanStatus::Ok,
            "success" => SpanStatus::Ok, // Backwards compat with initial schema
            "deadline_exceeded" => SpanStatus::DeadlineExceeded,
            "unauthenticated" => SpanStatus::Unauthenticated,
            "permission_denied" => SpanStatus::PermissionDenied,
            "not_found" => SpanStatus::NotFound,
            "resource_exhausted" => SpanStatus::ResourceExhausted,
            "invalid_argument" => SpanStatus::InvalidArgument,
            "unimplemented" => SpanStatus::Unimplemented,
            "unavailable" => SpanStatus::Unavailable,
            "internal_error" => SpanStatus::InternalError,
            "failure" => SpanStatus::InternalError, // Backwards compat with initial schema
            "unknown_error" => SpanStatus::UnknownError,
            "cancelled" => SpanStatus::Cancelled,
            "already_exists" => SpanStatus::AlreadyExists,
            "failed_precondition" => SpanStatus::FailedPrecondition,
            "aborted" => SpanStatus::Aborted,
            "out_of_range" => SpanStatus::OutOfRange,
            "data_loss" => SpanStatus::DataLoss,
            _ => return Err(ParseSpanStatusError),
        })
    }
}

impl fmt::Display for SpanStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SpanStatus::Ok => write!(f, "ok"),
            SpanStatus::DeadlineExceeded => write!(f, "deadline_exceeded"),
            SpanStatus::Unauthenticated => write!(f, "unauthenticated"),
            SpanStatus::PermissionDenied => write!(f, "permission_denied"),
            SpanStatus::NotFound => write!(f, "not_found"),
            SpanStatus::ResourceExhausted => write!(f, "resource_exhausted"),
            SpanStatus::InvalidArgument => write!(f, "invalid_argument"),
            SpanStatus::Unimplemented => write!(f, "unimplemented"),
            SpanStatus::Unavailable => write!(f, "unavailable"),
            SpanStatus::InternalError => write!(f, "internal_error"),
            SpanStatus::UnknownError => write!(f, "unknown_error"),
            SpanStatus::Cancelled => write!(f, "cancelled"),
            SpanStatus::AlreadyExists => write!(f, "already_exists"),
            SpanStatus::FailedPrecondition => write!(f, "failed_precondition"),
            SpanStatus::Aborted => write!(f, "aborted"),
            SpanStatus::OutOfRange => write!(f, "out_of_range"),
            SpanStatus::DataLoss => write!(f, "data_loss"),
        }
    }
}

impl FromValue for SpanStatus {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(status) => Annotated(Some(status), meta),
                Err(_) => {
                    meta.add_error(Error::expected("a trace status"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(value)), mut meta) => Annotated(
                Some(match value {
                    0 => SpanStatus::Ok,
                    1 => SpanStatus::Cancelled,
                    2 => SpanStatus::UnknownError,
                    3 => SpanStatus::InvalidArgument,
                    4 => SpanStatus::DeadlineExceeded,
                    5 => SpanStatus::NotFound,
                    6 => SpanStatus::AlreadyExists,
                    7 => SpanStatus::PermissionDenied,
                    8 => SpanStatus::ResourceExhausted,
                    9 => SpanStatus::FailedPrecondition,
                    10 => SpanStatus::Aborted,
                    11 => SpanStatus::OutOfRange,
                    12 => SpanStatus::Unimplemented,
                    13 => SpanStatus::InternalError,
                    14 => SpanStatus::Unavailable,
                    15 => SpanStatus::DataLoss,
                    16 => SpanStatus::Unauthenticated,
                    _ => {
                        meta.add_error(Error::expected("a trace status"));
                        meta.set_original_value(Some(value));
                        return Annotated(None, meta);
                    }
                }),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a string"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for SpanStatus {
    fn to_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}

impl TraceContext {
    /// The key under which a trace context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "trace"
    }
}

/// A context describes environment info (e.g. device, os or browser).
#[derive(Clone, Debug, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_context")]
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
    /// Information about device's GPU.
    Gpu(Box<GpuContext>),
    /// Information related to Monitors feature.
    Trace(Box<TraceContext>),
    /// Information related to Monitors feature.
    Monitor(Box<MonitorContext>),
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

impl Context {
    /// Represents the key under which a particular context type will be inserted in a Contexts object
    ///
    /// See [Contexts.add](struct.Contexts.html)
    pub fn default_key(&self) -> Option<&'static str> {
        match &self {
            Context::Device(_) => Some(DeviceContext::default_key()),
            Context::Os(_) => Some(OsContext::default_key()),
            Context::Runtime(_) => Some(RuntimeContext::default_key()),
            Context::App(_) => Some(AppContext::default_key()),
            Context::Browser(_) => Some(BrowserContext::default_key()),
            Context::Gpu(_) => Some(GpuContext::default_key()),
            Context::Trace(_) => Some(TraceContext::default_key()),
            Context::Monitor(_) => Some(MonitorContext::default_key()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct ContextInner(#[metastructure(bag_size = "large")] pub Context);

impl std::ops::Deref for ContextInner {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ContextInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Context> for ContextInner {
    fn from(c: Context) -> ContextInner {
        ContextInner(c)
    }
}

/// An object holding multiple contexts.
#[derive(Clone, Debug, PartialEq, Empty, ToValue, ProcessValue, Default)]
pub struct Contexts(pub Object<ContextInner>);

impl Contexts {
    pub fn new() -> Contexts {
        Contexts(Object::<ContextInner>::new())
    }

    /// Adds a context to self under the default key for the Context
    pub fn add(&mut self, context: Context) {
        if let Some(key) = context.default_key() {
            self.insert(key.to_owned(), Annotated::new(ContextInner(context)));
        }
    }

    /// Adds a context at the specified index,
    /// Prefer [add] when adding contexts with valid default_key()
    /// This method should be used for Other context or for contexts that are
    /// not added at their default keys
    pub fn add_at_index<S>(&mut self, key: S, context: Context)
    where
        S: Into<String>,
    {
        self.insert(key.into(), Annotated::new(ContextInner(context)));
    }

    /// Returns the context at the specified key or constructs it if not present.
    pub fn get_or_insert_with<F, S>(&mut self, key: S, context_builder: F) -> &mut Context
    where
        F: FnOnce() -> Context + 'static,
        S: Into<String>,
    {
        &mut self
            .0
            .entry(key.into())
            .or_insert_with(|| Annotated::empty())
            .value_mut()
            .get_or_insert_with(|| ContextInner(context_builder()))
            .0
    }
}

impl std::ops::Deref for Contexts {
    type Target = Object<ContextInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Contexts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

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
  "manufacturer": "Apple",
  "brand": "iphone",
  "screen_resolution": "800x600",
  "screen_density": 1.1,
  "screen_dpi": 1,
  "online": true,
  "charging": false,
  "low_memory": false,
  "simulator": true,
  "memory_size": 3137978368,
  "free_memory": 322781184,
  "usable_memory": 2843525120,
  "storage_size": 63989469184,
  "free_storage": 31994734592,
  "external_storage_size": 2097152,
  "external_free_storage": 2097152,
  "boot_time": "2018-02-08T12:52:12Z",
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
        manufacturer: Annotated::new("Apple".to_string()),
        brand: Annotated::new("iphone".to_string()),
        screen_resolution: Annotated::new("800x600".to_string()),
        screen_density: Annotated::new(1.1),
        screen_dpi: Annotated::new(1),
        online: Annotated::new(true),
        charging: Annotated::new(false),
        low_memory: Annotated::new(false),
        memory_size: Annotated::new(3_137_978_368),
        free_memory: Annotated::new(322_781_184),
        usable_memory: Annotated::new(2_843_525_120),
        storage_size: Annotated::new(63_989_469_184),
        free_storage: Annotated::new(31_994_734_592),
        external_storage_size: Annotated::new(2_097_152),
        external_free_storage: Annotated::new(2_097_152),
        boot_time: Annotated::new("2018-02-08T12:52:12Z".to_string()),
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
        build: Annotated::new(LenientString("FEEDFACE".to_string())),
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
  "build": "stable",
  "raw_description": "rustc 1.27.0 stable",
  "other": "value",
  "type": "runtime"
}"#;
    let context = Annotated::new(Context::Runtime(Box::new(RuntimeContext {
        name: Annotated::new("rustc".to_string()),
        version: Annotated::new("1.27.0".to_string()),
        build: Annotated::new(LenientString("stable".to_string())),
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
  "app_start_time": "2018-02-08T22:21:57Z",
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
        app_start_time: Annotated::new("2018-02-08T22:21:57Z".to_string()),
        device_app_hash: Annotated::new("4c793e3776474877ae30618378e9662a".to_string()),
        build_type: Annotated::new("testflight".to_string()),
        app_identifier: Annotated::new("foo.bar.baz".to_string()),
        app_name: Annotated::new("Baz App".to_string()),
        app_version: Annotated::new("1.0".to_string()),
        app_build: Annotated::new("100001".to_string().into()),
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
fn test_trace_context_roundtrip() {
    let json = r#"{
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "span_id": "fa90fdead5f74052",
  "parent_span_id": "fa90fdead5f74053",
  "op": "http",
  "status": "ok",
  "other": "value",
  "type": "trace"
}"#;
    let context = Annotated::new(Context::Trace(Box::new(TraceContext {
        trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
        span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
        parent_span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
        op: Annotated::new("http".into()),
        status: Annotated::new(SpanStatus::Ok),
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
fn test_trace_context_normalization() {
    let json = r#"{
  "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
  "span_id": "FA90FDEAD5F74052",
  "type": "trace"
}"#;
    let context = Annotated::new(Context::Trace(Box::new(TraceContext {
        trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
        span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
        ..Default::default()
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
}

#[test]
fn test_other_context_roundtrip() {
    use crate::types::Map;

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

    let os_context = Annotated::new(ContextInner(Context::Os(Box::new(OsContext {
        name: Annotated::new("Linux".to_string()),
        ..Default::default()
    }))));
    let mut map = Object::new();
    map.insert("os".to_string(), os_context);
    let contexts = Annotated::new(Contexts(map));

    assert_eq_dbg!(contexts, Annotated::from_json(json).unwrap());
}

#[test]
fn test_multiple_contexts_roundtrip() {
    let json = r#"{"os":{"name":"Linux","type":"os"},"runtime":{"name":"rustc","type":"runtime"}}"#;

    let os_context = Annotated::new(ContextInner(Context::Os(Box::new(OsContext {
        name: Annotated::new("Linux".to_string()),
        ..Default::default()
    }))));

    let runtime_context =
        Annotated::new(ContextInner(Context::Runtime(Box::new(RuntimeContext {
            name: Annotated::new("rustc".to_string()),
            ..Default::default()
        }))));

    let mut map = Object::new();
    map.insert("os".to_string(), os_context);
    map.insert("runtime".to_string(), runtime_context);
    let contexts = Annotated::new(Contexts(map));

    assert_eq_dbg!(contexts, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, contexts.to_json().unwrap());
}

#[test]
fn test_context_processing() {
    use crate::processor::{ProcessingState, Processor};
    use crate::protocol::Event;
    use crate::types::{Meta, ProcessingResult};

    let mut event = Annotated::new(Event {
        contexts: Annotated::new(Contexts({
            let mut contexts = Object::new();
            contexts.insert(
                "runtime".to_owned(),
                Annotated::new(ContextInner(Context::Runtime(Box::new(RuntimeContext {
                    name: Annotated::new("php".to_owned()),
                    version: Annotated::new("7.1.20-1+ubuntu16.04.1+deb.sury.org+1".to_owned()),
                    ..Default::default()
                })))),
            );
            contexts
        })),
        ..Default::default()
    });

    struct FooProcessor {
        called: bool,
    }

    impl Processor for FooProcessor {
        #[inline]
        fn process_context(
            &mut self,
            _value: &mut Context,
            _meta: &mut Meta,
            _state: &ProcessingState<'_>,
        ) -> ProcessingResult {
            self.called = true;
            Ok(())
        }
    }

    let mut processor = FooProcessor { called: false };
    crate::processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert!(processor.called);
}
