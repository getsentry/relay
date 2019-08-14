//! Implements the sentry event protocol.
mod breadcrumb;
mod clientsdk;
mod contexts;
mod debugmeta;
mod event;
mod exception;
mod fingerprint;
mod logentry;
mod mechanism;
mod request;
mod security_report;
mod span;
mod stacktrace;
mod tags;
mod templateinfo;
mod thread;
mod types;
mod user;

pub use self::breadcrumb::Breadcrumb;
pub use self::clientsdk::{ClientSdkInfo, ClientSdkPackage};
pub use self::contexts::{
    AppContext, BrowserContext, Context, ContextInner, Contexts, DeviceContext, OsContext,
    RuntimeContext, SpanId, TraceContext, TraceId,
};
pub use self::debugmeta::{
    AppleDebugImage, DebugImage, DebugMeta, NativeDebugImage, SystemSdkInfo,
};
pub use self::event::{
    Event, EventId, EventProcessingError, EventType, ExtraValue, GroupingConfig,
    ParseEventTypeError,
};
pub use self::exception::Exception;
pub use self::fingerprint::Fingerprint;
pub use self::logentry::LogEntry;
pub use self::mechanism::{CError, MachException, Mechanism, MechanismMeta, PosixSignal};
pub use self::request::{Cookies, HeaderName, Headers, Query, Request};
pub use self::security_report::{Csp, ExpectCt, ExpectStaple, Hpkp};
pub use self::span::Span;
pub use self::stacktrace::{Frame, FrameData, FrameVars, RawStacktrace, Stacktrace};
pub use self::tags::{TagEntry, Tags};
pub use self::templateinfo::TemplateInfo;
pub use self::thread::{Thread, ThreadId};
pub use self::types::{
    Addr, AsPair, InvalidRegVal, IpAddr, JsonLenientString, LenientString, Level, PairList,
    ParseLevelError, RegVal, Values,
};
pub use self::user::{Geo, User};
