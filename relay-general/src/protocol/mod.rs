//! Implements the sentry event protocol.

// FIXME: Workaround for https://github.com/GREsau/schemars/pull/65
#![allow(clippy::field_reassign_with_default)]

mod breadcrumb;
mod clientsdk;
mod constants;
mod contexts;
mod debugmeta;
mod event;
mod exception;
mod fingerprint;
mod logentry;
mod measurements;
mod mechanism;
mod metrics;
mod request;
#[cfg(feature = "jsonschema")]
mod schema;
mod security_report;
mod session;
mod span;
mod stacktrace;
mod tags;
mod templateinfo;
mod thread;
mod types;
mod user;
mod user_report;

pub use self::breadcrumb::Breadcrumb;
pub use self::clientsdk::{ClientSdkInfo, ClientSdkPackage};
pub use self::constants::{INVALID_ENVIRONMENTS, INVALID_RELEASES, VALID_PLATFORMS};
pub use self::contexts::{
    AppContext, BrowserContext, Context, ContextInner, Contexts, DeviceContext, GpuContext,
    OperationType, OsContext, RuntimeContext, SpanId, SpanStatus, TraceContext, TraceId,
};
pub use self::debugmeta::{
    AppleDebugImage, CodeId, DebugId, DebugImage, DebugMeta, NativeDebugImage, NativeImagePath,
    SystemSdkInfo,
};
pub use self::event::{
    Event, EventId, EventProcessingError, EventType, ExtraValue, GroupingConfig,
    ParseEventTypeError,
};
pub use self::exception::Exception;
pub use self::fingerprint::Fingerprint;
pub use self::logentry::{LogEntry, Message};
pub use self::measurements::Measurements;
pub use self::mechanism::{CError, MachException, Mechanism, MechanismMeta, PosixSignal};
pub use self::metrics::Metrics;
pub use self::request::{Cookies, HeaderName, HeaderValue, Headers, Query, Request};
#[cfg(feature = "jsonschema")]
pub use self::schema::event_json_schema;
pub use self::security_report::{Csp, ExpectCt, ExpectStaple, Hpkp, SecurityReportType};
pub use self::session::{
    ParseSessionStatusError, SessionAggregateItem, SessionAggregates, SessionAttributes,
    SessionStatus, SessionUpdate,
};
pub use self::span::Span;
pub use self::stacktrace::{Frame, FrameData, FrameVars, RawStacktrace, Stacktrace};
pub use self::tags::{TagEntry, Tags};
pub use self::templateinfo::TemplateInfo;
pub use self::thread::{Thread, ThreadId};
pub use self::types::{
    datetime_to_timestamp, Addr, AsPair, InvalidRegVal, IpAddr, JsonLenientString, LenientString,
    Level, PairList, ParseLevelError, RegVal, Timestamp, Values,
};
pub use self::user::{Geo, User};
pub use self::user_report::UserReport;
