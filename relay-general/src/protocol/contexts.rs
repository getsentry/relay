use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{Annotated, Empty, Error, FromValue, Object, SkipSerialization, ToValue, Value};

/// Device information.
///
/// Device context describes the device that caused the event. This is most appropriate for mobile
/// applications.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct DeviceContext {
    /// Name of the device.
    #[metastructure(pii = "maybe")]
    pub name: Annotated<String>,

    /// Family of the device model.
    ///
    /// This is usually the common part of model names across generations. For instance, `iPhone`
    /// would be a reasonable family, so would be `Samsung Galaxy`.
    pub family: Annotated<String>,

    /// Device model.
    ///
    /// This, for example, can be `Samsung Galaxy S3`.
    pub model: Annotated<String>,

    /// Device model (internal identifier).
    ///
    /// An internal hardware revision to identify the device exactly.
    pub model_id: Annotated<String>,

    /// Native cpu architecture of the device.
    pub arch: Annotated<String>,

    /// Current battery level in %.
    ///
    /// If the device has a battery, this can be a floating point value defining the battery level
    /// (in the range 0-100).
    pub battery_level: Annotated<f64>,

    /// Current screen orientation.
    ///
    /// This can be a string `portrait` or `landscape` to define the orientation of a device.
    pub orientation: Annotated<String>,

    /// Manufacturer of the device.
    pub manufacturer: Annotated<String>,

    /// Brand of the device.
    pub brand: Annotated<String>,

    /// Device screen resolution.
    ///
    /// (e.g.: 800x600, 3040x1444)
    #[metastructure(pii = "maybe")]
    pub screen_resolution: Annotated<String>,

    /// Device screen density.
    #[metastructure(pii = "maybe")]
    pub screen_density: Annotated<f64>,

    /// Screen density as dots-per-inch.
    #[metastructure(pii = "maybe")]
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
    #[metastructure(pii = "maybe")]
    pub memory_size: Annotated<u64>,

    /// How much memory is still available in bytes.
    #[metastructure(pii = "maybe")]
    pub free_memory: Annotated<u64>,

    /// How much memory is usable for the app in bytes.
    #[metastructure(pii = "maybe")]
    pub usable_memory: Annotated<u64>,

    /// Total storage size of the device in bytes.
    #[metastructure(pii = "maybe")]
    pub storage_size: Annotated<u64>,

    /// How much storage is free in bytes.
    #[metastructure(pii = "maybe")]
    pub free_storage: Annotated<u64>,

    /// Total size of the attached external storage in bytes (eg: android SDK card).
    #[metastructure(pii = "maybe")]
    pub external_storage_size: Annotated<u64>,

    /// Free size of the attached external storage in bytes (eg: android SDK card).
    #[metastructure(pii = "maybe")]
    pub external_free_storage: Annotated<u64>,

    /// Indicator when the device was booted.
    #[metastructure(pii = "maybe")]
    pub boot_time: Annotated<String>,

    /// Timezone of the device.
    #[metastructure(pii = "maybe")]
    pub timezone: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl DeviceContext {
    /// The key under which a device context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "device"
    }
}

/// Operating system information.
///
/// OS context describes the operating system on which the event was created. In web contexts, this
/// is the operating system of the browser (generally pulled from the User-Agent string).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct OsContext {
    /// Name of the operating system.
    pub name: Annotated<String>,

    /// Version of the operating system.
    pub version: Annotated<String>,

    /// Internal build number of the operating system.
    #[metastructure(pii = "maybe")]
    pub build: Annotated<LenientString>,

    /// Current kernel version.
    ///
    /// This is typically the entire output of the `uname` syscall.
    #[metastructure(pii = "maybe")]
    pub kernel_version: Annotated<String>,

    /// Indicator if the OS is rooted (mobile mostly).
    pub rooted: Annotated<bool>,

    /// Unprocessed operating system info.
    ///
    /// An unprocessed description string obtained by the operating system. For some well-known
    /// runtimes, Sentry will attempt to parse `name` and `version` from this string, if they are
    /// not explicitly given.
    #[metastructure(pii = "maybe")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl OsContext {
    /// The key under which an os context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "os"
    }
}

/// Runtime information.
///
/// Runtime context describes a runtime in more detail. Typically, this context is present in
/// `contexts` multiple times if multiple runtimes are involved (for instance, if you have a
/// JavaScript application running on top of JVM).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct RuntimeContext {
    /// Runtime name.
    pub name: Annotated<String>,

    /// Runtime version string.
    pub version: Annotated<String>,

    /// Application build string, if it is separate from the version.
    #[metastructure(pii = "maybe")]
    pub build: Annotated<LenientString>,

    /// Unprocessed runtime info.
    ///
    /// An unprocessed description string obtained by the runtime. For some well-known runtimes,
    /// Sentry will attempt to parse `name` and `version` from this string, if they are not
    /// explicitly given.
    #[metastructure(pii = "maybe")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl RuntimeContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "runtime"
    }
}

/// Application information.
///
/// App context describes the application. As opposed to the runtime, this is the actual
/// application that was running and carries metadata about the current session.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct AppContext {
    /// Start time of the app.
    ///
    /// Formatted UTC timestamp when the user started the application.
    #[metastructure(pii = "maybe")]
    pub app_start_time: Annotated<String>,

    /// Application-specific device identifier.
    #[metastructure(pii = "maybe")]
    pub device_app_hash: Annotated<String>,

    /// String identifying the kind of build. For example, `testflight`.
    pub build_type: Annotated<String>,

    /// Version-independent application identifier, often a dotted bundle ID.
    pub app_identifier: Annotated<String>,

    /// Application name as it appears on the platform.
    pub app_name: Annotated<String>,

    /// Application version as it appears on the platform.
    pub app_version: Annotated<String>,

    /// Internal build ID as it appears on the platform.
    pub app_build: Annotated<LenientString>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
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
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct BrowserContext {
    /// Display name of the browser application.
    pub name: Annotated<String>,

    /// Version string of the browser.
    pub version: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
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
    static ref MEASUREMENT_NAME: Regex = Regex::new("^[a-z0-9-._]+$").unwrap();
}

/// GPU information.
///
/// Example:
///
/// ```json
/// "gpu": {
///   "name": "AMD Radeon Pro 560",
///   "vendor_name": "Apple",
///   "memory_size": 4096,
///   "api_type": "Metal",
///   "multi_threaded_rendering": true,
///   "version": "Metal",
///   "npot_support": "Full"
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct GpuContext {
    /// The name of the graphics device.
    #[metastructure(pii = "maybe")]
    pub name: Annotated<String>,

    /// The Version of the graphics device.
    #[metastructure(pii = "maybe")]
    pub version: Annotated<String>,

    /// The PCI identifier of the graphics device.
    #[metastructure(pii = "maybe")]
    pub id: Annotated<Value>,

    /// The PCI vendor identifier of the graphics device.
    #[metastructure(pii = "maybe")]
    pub vendor_id: Annotated<String>,

    /// The vendor name as reported by the graphics device.
    #[metastructure(pii = "maybe")]
    pub vendor_name: Annotated<String>,

    /// The total GPU memory available in Megabytes.
    #[metastructure(pii = "maybe")]
    pub memory_size: Annotated<u64>,

    /// The device low-level API type.
    ///
    /// Examples: `"Apple Metal"` or `"Direct3D11"`
    #[metastructure(pii = "maybe")]
    pub api_type: Annotated<String>,

    /// Whether the GPU has multi-threaded rendering or not.
    #[metastructure(pii = "maybe")]
    pub multi_threaded_rendering: Annotated<bool>,

    /// The Non-Power-Of-Two support.
    #[metastructure(pii = "maybe")]
    pub npot_support: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl GpuContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "gpu"
    }
}

/// Monitor information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MonitorContext(#[metastructure(pii = "maybe")] pub Object<Value>);

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

/// Measurements context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MeasuresContext {
    /// measurements
    #[metastructure(retain = "true", skip_serialization = "never")]
    pub measurements: Annotated<Measurements>,
}

impl MeasuresContext {
    pub fn merge(&mut self, incoming_context: Annotated<MeasuresContext>) {
        let incoming_context = match incoming_context.into_value() {
            None => {
                return;
            }
            Some(context) => context,
        };

        match self.measurements.value_mut() {
            None => {
                self.measurements = incoming_context.measurements;
            }
            Some(Measurements(measurements)) => {
                let incoming_measurements = match incoming_context.measurements.into_value() {
                    None => {
                        return;
                    }
                    Some(Measurements(measurements)) => measurements,
                };

                for (name, value) in incoming_measurements.into_iter() {
                    measurements.insert(name, value);
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurements(pub Object<f64>);

impl FromValue for Measurements {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), mut meta) => {
                let mut measurements = Object::<f64>::new();

                for (raw_name, raw_value) in items.into_iter() {
                    let value = match raw_value {
                        Annotated(Some(Value::I64(value)), meta) => {
                            Annotated(Some(value as f64), meta)
                        }
                        Annotated(Some(Value::U64(value)), meta) => {
                            Annotated(Some(value as f64), meta)
                        }
                        Annotated(Some(Value::F64(value)), meta) => Annotated(Some(value), meta),
                        Annotated(None, meta) => Annotated(None, meta),
                        Annotated(Some(value), mut meta) => {
                            meta.add_error(Error::expected("number"));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    };

                    let measurement_name = raw_name.to_lowercase();

                    if !MEASUREMENT_NAME.is_match(&measurement_name) {
                        meta.add_error(Error::expected(
                            "measurement name to contain only characters a-zA-Z0-9-_.",
                        ));
                        meta.set_original_value(Some(raw_name));

                        // TODO: fix this
                    }

                    measurements.insert(measurement_name, value);
                }

                Annotated(Some(Measurements(measurements)), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("measurements"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// A 32-character hex string as described in the W3C trace context spec.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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

/// A 16-character hex string as described in the W3C trace context spec.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct TraceContext {
    /// The trace ID.
    #[metastructure(required = "true")]
    pub trace_id: Annotated<TraceId>,

    /// The ID of the span.
    #[metastructure(required = "true")]
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
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

#[doc(inline)]
pub use relay_common::{ParseSpanStatusError, SpanStatus};

impl ProcessValue for SpanStatus {}

impl Empty for SpanStatus {
    #[inline]
    fn is_empty(&self) -> bool {
        false
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
                    2 => SpanStatus::Unknown,
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
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
    /// Information related to Tracing.
    Trace(Box<TraceContext>),
    /// Information related to Monitors feature.
    Monitor(Box<MonitorContext>),
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(fallback_variant)]
    Other(#[metastructure(pii = "true")] Object<Value>),
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
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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

/// The Contexts Interface provides additional context data. Typically, this is data related to the
/// current user and the environment. For example, the device or application version. Its canonical
/// name is `contexts`.
///
/// The `contexts` type can be used to define arbitrary contextual data on the event. It accepts an
/// object of key/value pairs. The key is the “alias” of the context and can be freely chosen.
/// However, as per policy, it should match the type of the context unless there are two values for
/// a type. You can omit `type` if the key name is the type.
///
/// Unknown data for the contexts is rendered as a key/value list.
///
/// For more details about sending additional data with your event, see the [full documentation on
/// Additional Data](https://docs.sentry.io/enriching-error-data/additional-data/).
#[derive(Clone, Debug, PartialEq, Empty, ToValue, ProcessValue, Default)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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

    /// Returns the context at the specified key or constructs it if not present.
    pub fn get_or_insert_with<F, S>(&mut self, key: S, context_builder: F) -> &mut Context
    where
        F: FnOnce() -> Context,
        S: Into<String>,
    {
        &mut *self
            .entry(key.into())
            .or_insert_with(Annotated::empty)
            .value_mut()
            .get_or_insert_with(|| ContextInner(context_builder()))
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
