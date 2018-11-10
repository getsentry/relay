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
mod stacktrace;
mod tags;
mod templateinfo;
mod thread;
mod types;
mod user;

pub use self::breadcrumb::Breadcrumb;
pub use self::clientsdk::{ClientSdkInfo, ClientSdkPackage};
pub use self::contexts::{
    AppContext, BrowserContext, Context, Contexts, DeviceContext, OsContext, RuntimeContext,
};
pub use self::debugmeta::{
    AppleDebugImage, DebugImage, DebugMeta, SymbolicDebugImage, SystemSdkInfo,
};
pub use self::event::{Event, EventId, EventProcessingError, EventType, ParseEventTypeError};
pub use self::exception::Exception;
pub use self::fingerprint::Fingerprint;
pub use self::logentry::LogEntry;
pub use self::mechanism::{
    normalize_mechanism_meta, CError, MachException, Mechanism, MechanismMeta, OsHint, PosixSignal,
};
pub use self::request::{Cookies, Headers, Query, Request};
pub use self::stacktrace::{Frame, Stacktrace};
pub use self::tags::Tags;
pub use self::templateinfo::TemplateInfo;
pub use self::thread::Thread;
pub use self::types::{
    Addr, InvalidRegVal, IpAddr, JsonLenientString, LenientString, Level, ParseLevelError, RegVal,
    ThreadId, Values,
};
pub use self::user::{Geo, User};
