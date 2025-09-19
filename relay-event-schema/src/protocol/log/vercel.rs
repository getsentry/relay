use crate::processor::ProcessValue;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, SkipSerialization, Value};
use std::fmt::{self, Display};

/// Vercel log entry representing a single log from Vercel deployments.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct VercelLog {
    /// Unique identifier for the log entry
    #[metastructure(required = true)]
    pub id: Annotated<String>,

    /// Identifier for the Vercel deployment
    #[metastructure(required = true)]
    pub deployment_id: Annotated<String>,

    /// Origin of the log
    #[metastructure(required = true)]
    pub source: Annotated<VercelLogSource>,

    /// Deployment unique URL hostname
    #[metastructure(required = true)]
    pub host: Annotated<String>,

    /// Unix timestamp when the log was generated
    #[metastructure(required = true)]
    pub timestamp: Annotated<u64>,

    /// Identifier for the Vercel project
    #[metastructure(required = true)]
    pub project_id: Annotated<String>,

    /// Log severity level
    #[metastructure(required = true)]
    pub level: Annotated<VercelLogLevel>,

    /// Log message content (may be truncated if over 256 KB)
    #[metastructure(required = false, pii = "true", trim = false)]
    pub message: Annotated<String>,

    /// Identifier for the Vercel build (only present on build logs)
    #[metastructure(required = false)]
    pub build_id: Annotated<String>,

    /// Entrypoint for the request
    #[metastructure(required = false)]
    pub entrypoint: Annotated<String>,

    /// Origin of the external content (only on external logs)
    #[metastructure(required = false)]
    pub destination: Annotated<String>,

    /// Function or dynamic path of the request
    #[metastructure(required = false)]
    pub path: Annotated<String>,

    /// Log output type
    #[metastructure(required = false)]
    pub log_type: Annotated<String>,

    /// HTTP status code of the request (-1 means no response returned and the lambda crashed)
    #[metastructure(required = false)]
    pub status_code: Annotated<i32>,

    /// Identifier of the request
    #[metastructure(required = false)]
    pub request_id: Annotated<String>,

    /// Deployment environment
    #[metastructure(required = false)]
    pub environment: Annotated<VercelEnvironment>,

    /// Git branch name
    #[metastructure(required = false)]
    pub branch: Annotated<String>,

    /// JA3 fingerprint digest
    #[metastructure(required = false)]
    pub ja3_digest: Annotated<String>,

    /// JA4 fingerprint digest
    #[metastructure(required = false)]
    pub ja4_digest: Annotated<String>,

    /// Type of edge runtime
    #[metastructure(required = false)]
    pub edge_type: Annotated<VercelEdgeType>,

    /// Name of the Vercel project
    #[metastructure(required = false)]
    pub project_name: Annotated<String>,

    /// Region where the request is executed
    #[metastructure(required = false)]
    pub execution_region: Annotated<String>,

    /// Trace identifier for distributed tracing
    #[metastructure(required = false, trim = false)]
    pub trace_id: Annotated<String>,

    /// Span identifier for distributed tracing
    #[metastructure(required = false, trim = false)]
    pub span_id: Annotated<String>,

    /// Trace (alternative format)
    #[metastructure(required = false, trim = false)]
    #[serde(rename = "trace.id")]
    pub trace_dot_id: Annotated<String>,

    /// Span (alternative format)
    #[metastructure(required = false, trim = false)]
    #[serde(rename = "span.id")]
    pub span_dot_id: Annotated<String>,

    /// Contains information about proxy requests
    #[metastructure(required = false)]
    pub proxy: Annotated<VercelProxy>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

/// Information about proxy requests in Vercel logs
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    IntoValue,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct VercelProxy {
    /// Unix timestamp when the proxy request was made
    #[metastructure(required = true)]
    pub timestamp: Annotated<u64>,

    /// HTTP method of the request
    #[metastructure(required = true)]
    pub method: Annotated<String>,

    /// Hostname of the request
    #[metastructure(required = true)]
    pub host: Annotated<String>,

    /// Request path with query parameters
    #[metastructure(required = true)]
    pub path: Annotated<String>,

    /// User agent strings of the request
    #[metastructure(required = true)]
    pub user_agent: Annotated<Vec<String>>,

    /// Referer of the request
    #[metastructure(required = true)]
    pub referer: Annotated<String>,

    /// Region where the request is processed
    #[metastructure(required = true)]
    pub region: Annotated<String>,

    /// HTTP status code of the proxy request (-1 means revalidation occurred in the background)
    #[metastructure(required = false)]
    pub status_code: Annotated<i32>,

    /// Client IP address
    #[metastructure(required = false)]
    pub client_ip: Annotated<String>,

    /// Protocol of the request
    #[metastructure(required = false)]
    pub scheme: Annotated<String>,

    /// Size of the response in bytes
    #[metastructure(required = false)]
    pub response_byte_size: Annotated<u64>,

    /// Original request ID when request is served from cache
    #[metastructure(required = false)]
    pub cache_id: Annotated<String>,

    /// How the request was served based on its path and project configuration
    #[metastructure(required = false)]
    pub path_type: Annotated<VercelPathType>,

    /// Variant of the path type
    #[metastructure(required = false)]
    pub path_type_variant: Annotated<String>,

    /// Vercel-specific identifier
    #[metastructure(required = false)]
    pub vercel_id: Annotated<String>,

    /// Cache status sent to the browser
    #[metastructure(required = false)]
    pub vercel_cache: Annotated<VercelCache>,

    /// Region where lambda function executed
    #[metastructure(required = false)]
    pub lambda_region: Annotated<String>,

    /// Action taken by firewall rules
    #[metastructure(required = false)]
    pub waf_action: Annotated<VercelWafAction>,

    /// ID of the firewall rule that matched
    #[metastructure(required = false)]
    pub waf_rule_id: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

/// Origin of the log in Vercel
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelLogSource {
    Build,
    Edge,
    Lambda,
    Static,
    External,
    Firewall,
    /// Unknown source, for forward compatibility.
    Unknown(String),
}

impl VercelLogSource {
    fn as_str(&self) -> &str {
        match self {
            VercelLogSource::Build => "build",
            VercelLogSource::Edge => "edge",
            VercelLogSource::Lambda => "lambda",
            VercelLogSource::Static => "static",
            VercelLogSource::External => "external",
            VercelLogSource::Firewall => "firewall",
            VercelLogSource::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelLogSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelLogSource {
    fn from(value: String) -> Self {
        match value.as_str() {
            "build" => VercelLogSource::Build,
            "edge" => VercelLogSource::Edge,
            "lambda" => VercelLogSource::Lambda,
            "static" => VercelLogSource::Static,
            "external" => VercelLogSource::External,
            "firewall" => VercelLogSource::Firewall,
            _ => VercelLogSource::Unknown(value),
        }
    }
}

impl FromValue for VercelLogSource {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelLogSource::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelLogSource {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelLogSource {}

impl Empty for VercelLogSource {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Log severity level in Vercel
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelLogLevel {
    Info,
    Warning,
    Error,
    Fatal,
    /// Unknown level, for forward compatibility.
    Unknown(String),
}

impl VercelLogLevel {
    fn as_str(&self) -> &str {
        match self {
            VercelLogLevel::Info => "info",
            VercelLogLevel::Warning => "warning",
            VercelLogLevel::Error => "error",
            VercelLogLevel::Fatal => "fatal",
            VercelLogLevel::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelLogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelLogLevel {
    fn from(value: String) -> Self {
        match value.as_str() {
            "info" => VercelLogLevel::Info,
            "warning" => VercelLogLevel::Warning,
            "error" => VercelLogLevel::Error,
            "fatal" => VercelLogLevel::Fatal,
            _ => VercelLogLevel::Unknown(value),
        }
    }
}

impl FromValue for VercelLogLevel {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelLogLevel::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelLogLevel {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelLogLevel {}

impl Empty for VercelLogLevel {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Deployment environment in Vercel
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelEnvironment {
    Production,
    Preview,
    /// Unknown environment, for forward compatibility.
    Unknown(String),
}

impl VercelEnvironment {
    fn as_str(&self) -> &str {
        match self {
            VercelEnvironment::Production => "production",
            VercelEnvironment::Preview => "preview",
            VercelEnvironment::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelEnvironment {
    fn from(value: String) -> Self {
        match value.as_str() {
            "production" => VercelEnvironment::Production,
            "preview" => VercelEnvironment::Preview,
            _ => VercelEnvironment::Unknown(value),
        }
    }
}

impl FromValue for VercelEnvironment {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelEnvironment::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelEnvironment {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelEnvironment {}

impl Empty for VercelEnvironment {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Type of edge runtime in Vercel
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelEdgeType {
    EdgeFunction,
    Middleware,
    /// Unknown edge type, for forward compatibility.
    Unknown(String),
}

impl VercelEdgeType {
    fn as_str(&self) -> &str {
        match self {
            VercelEdgeType::EdgeFunction => "edge-function",
            VercelEdgeType::Middleware => "middleware",
            VercelEdgeType::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelEdgeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelEdgeType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "edge-function" => VercelEdgeType::EdgeFunction,
            "middleware" => VercelEdgeType::Middleware,
            _ => VercelEdgeType::Unknown(value),
        }
    }
}

impl FromValue for VercelEdgeType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelEdgeType::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelEdgeType {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelEdgeType {}

impl Empty for VercelEdgeType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// How the request was served based on its path and project configuration
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelPathType {
    Func,
    Prerender,
    BackgroundFunc,
    Edge,
    Middleware,
    StreamingFunc,
    PartialPrerender,
    External,
    Static,
    NotFound,
    Api,
    Unknown(String),
}

impl VercelPathType {
    fn as_str(&self) -> &str {
        match self {
            VercelPathType::Func => "func",
            VercelPathType::Prerender => "prerender",
            VercelPathType::BackgroundFunc => "background_func",
            VercelPathType::Edge => "edge",
            VercelPathType::Middleware => "middleware",
            VercelPathType::StreamingFunc => "streaming_func",
            VercelPathType::PartialPrerender => "partial_prerender",
            VercelPathType::External => "external",
            VercelPathType::Static => "static",
            VercelPathType::NotFound => "not_found",
            VercelPathType::Api => "api",
            VercelPathType::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelPathType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelPathType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "func" => VercelPathType::Func,
            "prerender" => VercelPathType::Prerender,
            "background_func" => VercelPathType::BackgroundFunc,
            "edge" => VercelPathType::Edge,
            "middleware" => VercelPathType::Middleware,
            "streaming_func" => VercelPathType::StreamingFunc,
            "partial_prerender" => VercelPathType::PartialPrerender,
            "external" => VercelPathType::External,
            "static" => VercelPathType::Static,
            "not_found" => VercelPathType::NotFound,
            "api" => VercelPathType::Api,
            "unknown" => VercelPathType::Unknown("unknown".to_string()),
            _ => VercelPathType::Unknown(value),
        }
    }
}

impl FromValue for VercelPathType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelPathType::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelPathType {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelPathType {}

impl Empty for VercelPathType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Cache status sent to the browser
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelCache {
    Miss,
    Hit,
    Stale,
    Bypass,
    Prerender,
    Revalidated,
    /// Unknown cache status, for forward compatibility.
    Unknown(String),
}

impl VercelCache {
    fn as_str(&self) -> &str {
        match self {
            VercelCache::Miss => "MISS",
            VercelCache::Hit => "HIT",
            VercelCache::Stale => "STALE",
            VercelCache::Bypass => "BYPASS",
            VercelCache::Prerender => "PRERENDER",
            VercelCache::Revalidated => "REVALIDATED",
            VercelCache::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelCache {
    fn from(value: String) -> Self {
        match value.as_str() {
            "MISS" => VercelCache::Miss,
            "HIT" => VercelCache::Hit,
            "STALE" => VercelCache::Stale,
            "BYPASS" => VercelCache::Bypass,
            "PRERENDER" => VercelCache::Prerender,
            "REVALIDATED" => VercelCache::Revalidated,
            _ => VercelCache::Unknown(value),
        }
    }
}

impl FromValue for VercelCache {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelCache::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelCache {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelCache {}

impl Empty for VercelCache {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// Action taken by firewall rules
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VercelWafAction {
    Log,
    Challenge,
    Deny,
    Bypass,
    RateLimit,
    /// Unknown WAF action, for forward compatibility.
    Unknown(String),
}

impl VercelWafAction {
    fn as_str(&self) -> &str {
        match self {
            VercelWafAction::Log => "log",
            VercelWafAction::Challenge => "challenge",
            VercelWafAction::Deny => "deny",
            VercelWafAction::Bypass => "bypass",
            VercelWafAction::RateLimit => "rate_limit",
            VercelWafAction::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for VercelWafAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for VercelWafAction {
    fn from(value: String) -> Self {
        match value.as_str() {
            "log" => VercelWafAction::Log,
            "challenge" => VercelWafAction::Challenge,
            "deny" => VercelWafAction::Deny,
            "bypass" => VercelWafAction::Bypass,
            "rate_limit" => VercelWafAction::RateLimit,
            _ => VercelWafAction::Unknown(value),
        }
    }
}

impl FromValue for VercelWafAction {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(VercelWafAction::from(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for VercelWafAction {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for VercelWafAction {}

impl Empty for VercelWafAction {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}
