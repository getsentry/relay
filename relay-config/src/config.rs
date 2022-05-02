use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use failure::{Backtrace, Context, Fail};
use serde::de::{Unexpected, Visitor};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};

use relay_auth::{generate_key_pair, generate_relay_id, PublicKey, RelayId, SecretKey};
use relay_common::Uuid;
use relay_metrics::AggregatorConfig;
use relay_redis::RedisConfig;

use crate::byte_size::ByteSize;
use crate::upstream::UpstreamDescriptor;

const DEFAULT_NETWORK_OUTAGE_GRACE_PERIOD: u64 = 10;

/// Defines the source of a config error
#[derive(Debug)]
enum ConfigErrorSource {
    /// An error occurring independently.
    None,
    /// An error originating from a configuration file.
    File(PathBuf),
    /// An error originating in a field override (an env var, or a CLI parameter).
    FieldOverride(String),
}

impl Default for ConfigErrorSource {
    fn default() -> Self {
        Self::None
    }
}

/// Indicates config related errors.
#[derive(Debug)]
pub struct ConfigError {
    source: ConfigErrorSource,
    inner: Context<ConfigErrorKind>,
}

impl ConfigError {
    #[inline]
    fn new<C>(context: C) -> Self
    where
        C: Into<Context<ConfigErrorKind>>,
    {
        Self {
            source: ConfigErrorSource::None,
            inner: context.into(),
        }
    }

    #[inline]
    fn wrap<E>(inner: E, kind: ConfigErrorKind) -> Self
    where
        E: Fail,
    {
        Self::new(inner.context(kind))
    }

    #[inline]
    fn for_field<E>(inner: E, field: &'static str) -> Self
    where
        E: Fail,
    {
        Self::wrap(inner, ConfigErrorKind::InvalidValue).field(field)
    }

    #[inline]
    fn file<P: AsRef<Path>>(mut self, p: P) -> Self {
        self.source = ConfigErrorSource::File(p.as_ref().to_path_buf());
        self
    }

    #[inline]
    fn field(mut self, name: &'static str) -> Self {
        self.source = ConfigErrorSource::FieldOverride(name.to_owned());
        self
    }

    /// Returns the error kind of the error.
    pub fn kind(&self) -> ConfigErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for ConfigError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            ConfigErrorSource::None => self.inner.fmt(f),
            ConfigErrorSource::File(file_name) => {
                write!(f, "{} (file {})", self.inner, file_name.display())
            }
            ConfigErrorSource::FieldOverride(name) => write!(f, "{} (field {})", self.inner, name),
        }
    }
}

/// Indicates config related errors.
#[derive(Fail, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ConfigErrorKind {
    /// Failed to open the file.
    #[fail(display = "could not open config file")]
    CouldNotOpenFile,
    /// Failed to save a file.
    #[fail(display = "could not write config file")]
    CouldNotWriteFile,
    /// Parsing YAML failed.
    #[fail(display = "could not parse yaml config file")]
    BadYaml,
    /// Parsing JSON failed.
    #[fail(display = "could not parse json config file")]
    BadJson,
    /// Invalid config value
    #[fail(display = "invalid config value")]
    InvalidValue,
    /// The user attempted to run Relay with processing enabled, but uses a binary that was
    /// compiled without the processing feature.
    #[fail(display = "was not compiled with processing, cannot enable processing")]
    ProcessingNotAvailable,
    /// The user referenced a kafka config name that does not exist.
    #[fail(display = "unknown kafka config name")]
    UnknownKafkaConfigName,
}

enum ConfigFormat {
    Yaml,
    Json,
}

impl ConfigFormat {
    pub fn extension(&self) -> &'static str {
        match self {
            ConfigFormat::Yaml => "yml",
            ConfigFormat::Json => "json",
        }
    }
}

trait ConfigObject: DeserializeOwned + Serialize {
    /// The format in which to serialize this configuration.
    fn format() -> ConfigFormat;

    /// The basename of the config file.
    fn name() -> &'static str;

    /// The full filename of the config file, including the file extension.
    fn path(base: &Path) -> PathBuf {
        base.join(format!("{}.{}", Self::name(), Self::format().extension()))
    }

    /// Loads the config file from a file within the given directory location.
    fn load(base: &Path) -> Result<Self, ConfigError> {
        let path = Self::path(base);

        let f = fs::File::open(&path)
            .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotOpenFile).file(&path))?;

        match Self::format() {
            ConfigFormat::Yaml => serde_yaml::from_reader(io::BufReader::new(f))
                .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::BadYaml).file(&path)),
            ConfigFormat::Json => serde_json::from_reader(io::BufReader::new(f))
                .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::BadJson).file(&path)),
        }
    }

    /// Writes the configuration object to the given writer.
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), ConfigError> {
        match Self::format() {
            ConfigFormat::Yaml => serde_yaml::to_writer(writer, self)
                .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotWriteFile)),
            ConfigFormat::Json => serde_json::to_writer_pretty(writer, self)
                .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotWriteFile)),
        }
    }

    /// Writes the configuration to a file within the given directory location.
    fn save(&self, base: &Path) -> Result<(), ConfigError> {
        let path = Self::path(base);
        let mut options = fs::OpenOptions::new();
        options.write(true).truncate(true).create(true);

        // Remove all non-user permissions for the newly created file
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let mut f = options
            .open(&path)
            .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotWriteFile).file(&path))?;

        self.write(&mut f).map_err(|e| e.file(&path))?;
        f.write_all(b"\n").ok();

        Ok(())
    }
}

/// Structure used to hold information about configuration overrides via
/// CLI parameters or environment variables
#[derive(Debug, Default)]
pub struct OverridableConfig {
    /// The operation mode of this relay.
    pub mode: Option<String>,
    /// The upstream relay or sentry instance.
    pub upstream: Option<String>,
    /// The host the relay should bind to (network interface).
    pub host: Option<String>,
    /// The port to bind for the unencrypted relay HTTP server.
    pub port: Option<String>,
    /// "true" if processing is enabled "false" otherwise
    pub processing: Option<String>,
    /// the kafka bootstrap.servers configuration string
    pub kafka_url: Option<String>,
    /// the redis server url
    pub redis_url: Option<String>,
    /// The globally unique ID of the relay.
    pub id: Option<String>,
    /// The secret key of the relay
    pub secret_key: Option<String>,
    /// The public key of the relay
    pub public_key: Option<String>,
    /// Outcome source
    pub outcome_source: Option<String>,
    /// shutdown timeout
    pub shutdown_timeout: Option<String>,
}

/// The relay credentials
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The secret key of the relay
    pub secret_key: SecretKey,
    /// The public key of the relay
    pub public_key: PublicKey,
    /// The globally unique ID of the relay.
    pub id: RelayId,
}

impl Credentials {
    /// Generates new random credentials.
    pub fn generate() -> Self {
        relay_log::info!("generating new relay credentials");
        let (sk, pk) = generate_key_pair();
        Self {
            secret_key: sk,
            public_key: pk,
            id: generate_relay_id(),
        }
    }

    /// Serializes this configuration to JSON.
    pub fn to_json_string(&self) -> Result<String, ConfigError> {
        serde_json::to_string(self)
            .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotWriteFile))
    }
}

impl ConfigObject for Credentials {
    fn format() -> ConfigFormat {
        ConfigFormat::Json
    }
    fn name() -> &'static str {
        "credentials"
    }
}

/// Information on a downstream Relay.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RelayInfo {
    /// The public key that this Relay uses to authenticate and sign requests.
    pub public_key: PublicKey,

    /// Marks an internal relay that has privileged access to more project configuration.
    #[serde(default)]
    pub internal: bool,
}

impl RelayInfo {
    /// Creates a new RelayInfo
    pub fn new(public_key: PublicKey) -> Self {
        Self {
            public_key,
            internal: false,
        }
    }
}

/// The operation mode of a relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RelayMode {
    /// This relay acts as a proxy for all requests and events.
    ///
    /// Events are normalized and rate limits from the upstream are enforced, but the relay will not
    /// fetch project configurations from the upstream or perform PII stripping. All events are
    /// accepted unless overridden on the file system.
    Proxy,

    /// This relay is configured statically in the file system.
    ///
    /// Events are only accepted for projects configured statically in the file system. All other
    /// events are rejected. If configured, PII stripping is also performed on those events.
    Static,

    /// Project configurations are managed by the upstream.
    ///
    /// Project configurations are always fetched from the upstream, unless they are statically
    /// overridden in the file system. This relay must be allowed in the upstream Sentry. This is
    /// only possible, if the upstream is Sentry directly, or another managed Relay.
    Managed,

    /// Events are held in memory for inspection only.
    ///
    /// This mode is used for testing sentry SDKs.
    Capture,
}

impl fmt::Display for RelayMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RelayMode::Proxy => write!(f, "proxy"),
            RelayMode::Static => write!(f, "static"),
            RelayMode::Managed => write!(f, "managed"),
            RelayMode::Capture => write!(f, "capture"),
        }
    }
}

impl FromStr for RelayMode {
    type Err = Context<&'static str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "proxy" => Ok(RelayMode::Proxy),
            "static" => Ok(RelayMode::Static),
            "managed" => Ok(RelayMode::Managed),
            "capture" => Ok(RelayMode::Capture),
            _ => Err(Context::new(
                "Relay mode must be one of: managed, static, proxy, capture",
            )),
        }
    }
}

/// Returns `true` if this value is equal to `Default::default()`.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

/// Checks if we are running in docker.
fn is_docker() -> bool {
    if fs::metadata("/.dockerenv").is_ok() {
        return true;
    }

    fs::read_to_string("/proc/self/cgroup").map_or(false, |s| s.contains("/docker"))
}

/// Default value for the "bind" configuration.
fn default_host() -> IpAddr {
    if is_docker() {
        // Docker images rely on this service being exposed
        "0.0.0.0".parse().unwrap()
    } else {
        "127.0.0.1".parse().unwrap()
    }
}

/// Controls responses from the readiness health check endpoint based on authentication.
///
/// Independent of the the readiness condition, shutdown always switches Relay into unready state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadinessCondition {
    /// (default) Relay is ready when authenticated and connected to the upstream.
    ///
    /// Before authentication has succeeded and during network outages, Relay responds as not ready.
    /// Relay reauthenticates based on the `http.auth_interval` parameter. During reauthentication,
    /// Relay remains ready until authentication fails.
    ///
    /// Authentication is only required for Relays in managed mode. Other Relays will only check for
    /// network outages.
    Authenticated,
    /// Relay reports readiness regardless of the authentication and networking state.
    Always,
}

impl Default for ReadinessCondition {
    fn default() -> Self {
        Self::Authenticated
    }
}

/// Relay specific configuration values.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Relay {
    /// The operation mode of this relay.
    pub mode: RelayMode,
    /// The upstream relay or sentry instance.
    pub upstream: UpstreamDescriptor<'static>,
    /// The host the relay should bind to (network interface).
    pub host: IpAddr,
    /// The port to bind for the unencrypted relay HTTP server.
    pub port: u16,
    /// Optional port to bind for the encrypted relay HTTPS server.
    pub tls_port: Option<u16>,
    /// The path to the identity (DER-encoded PKCS12) to use for TLS.
    pub tls_identity_path: Option<PathBuf>,
    /// Password for the PKCS12 archive.
    pub tls_identity_password: Option<String>,
    /// Always override project IDs from the URL and DSN with the identifier used at the upstream.
    ///
    /// Enable this setting for Relays used to redirect traffic to a migrated Sentry instance.
    /// Validation of project identifiers can be safely skipped in these cases.
    #[serde(skip_serializing_if = "is_default")]
    pub override_project_ids: bool,
}

impl Default for Relay {
    fn default() -> Self {
        Relay {
            mode: RelayMode::Managed,
            upstream: "https://sentry.io/".parse().unwrap(),
            host: default_host(),
            port: 3000,
            tls_port: None,
            tls_identity_path: None,
            tls_identity_password: None,
            override_project_ids: false,
        }
    }
}

/// Control the metrics.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Metrics {
    /// Hostname and port of the statsd server.
    ///
    /// Defaults to `None`.
    statsd: Option<String>,
    /// Common prefix that should be added to all metrics.
    ///
    /// Defaults to `"sentry.relay"`.
    prefix: String,
    /// Default tags to apply to all metrics.
    default_tags: BTreeMap<String, String>,
    /// Tag name to report the hostname to for each metric. Defaults to not sending such a tag.
    hostname_tag: Option<String>,
    /// Emitted metrics will be buffered to optimize performance.
    ///
    /// Defaults to `true`.
    buffering: bool,
    /// Global sample rate for all emitted metrics between `0.0` and `1.0`.
    ///
    /// For example, a value of `0.3` means that only 30% of the emitted metrics will be sent.
    /// Defaults to `1.0` (100%).
    sample_rate: f32,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            statsd: None,
            prefix: "sentry.relay".into(),
            default_tags: BTreeMap::new(),
            hostname_tag: None,
            buffering: true,
            sample_rate: 1.0,
        }
    }
}

/// Controls various limits
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Limits {
    /// How many requests can be sent concurrently from Relay to the upstream before Relay starts
    /// buffering.
    max_concurrent_requests: usize,
    /// How many queries can be sent concurrently from Relay to the upstream before Relay starts
    /// buffering.
    ///
    /// The concurrency of queries is additionally constrained by `max_concurrent_requests`.
    max_concurrent_queries: usize,
    /// The maximum payload size for events.
    max_event_size: ByteSize,
    /// The maximum size for each attachment.
    max_attachment_size: ByteSize,
    /// The maximum combined size for all attachments in an envelope or request.
    max_attachments_size: ByteSize,
    /// The maximum combined size for all client reports in an envelope or request.
    max_client_reports_size: ByteSize,
    /// The maximum payload size for an entire envelopes. Individual limits still apply.
    max_envelope_size: ByteSize,
    /// The maximum number of session items per envelope.
    max_session_count: usize,
    /// The maximum payload size for general API requests.
    max_api_payload_size: ByteSize,
    /// The maximum payload size for file uploads and chunks.
    max_api_file_upload_size: ByteSize,
    /// The maximum payload size for chunks
    max_api_chunk_upload_size: ByteSize,
    /// The maximum payload size for a profile
    max_profile_size: ByteSize,
    /// The maximum number of threads to spawn for CPU and web work, each.
    ///
    /// The total number of threads spawned will roughly be `2 * max_thread_count + 1`. Defaults to
    /// the number of logical CPU cores on the host.
    max_thread_count: usize,
    /// The maximum number of seconds a query is allowed to take across retries. Individual requests
    /// have lower timeouts. Defaults to 30 seconds.
    query_timeout: u64,
    /// The maximum number of connections to Relay that can be created at once.
    max_connection_rate: usize,
    /// The maximum number of pending connects to Relay. This corresponds to the backlog param of
    /// `listen(2)` in POSIX.
    max_pending_connections: i32,
    /// The maximum number of open connections to Relay.
    max_connections: usize,
    /// The maximum number of seconds to wait for pending envelopes after receiving a shutdown
    /// signal.
    shutdown_timeout: u64,
    /// The maximum memory allocation Relay can make, in bytes.
    ///
    /// Purposefully undocumented because we don't know yet how feasible this is.
    #[serde(rename = "_max_alloc_size")]
    max_alloc_size: Option<ByteSize>,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_concurrent_requests: 100,
            max_concurrent_queries: 5,
            max_event_size: ByteSize::mebibytes(1),
            max_attachment_size: ByteSize::mebibytes(100),
            max_attachments_size: ByteSize::mebibytes(100),
            max_client_reports_size: ByteSize::kibibytes(4),
            max_envelope_size: ByteSize::mebibytes(100),
            max_session_count: 100,
            max_api_payload_size: ByteSize::mebibytes(20),
            max_api_file_upload_size: ByteSize::mebibytes(40),
            max_api_chunk_upload_size: ByteSize::mebibytes(100),
            max_profile_size: ByteSize::mebibytes(10),
            max_thread_count: num_cpus::get(),
            query_timeout: 30,
            max_connection_rate: 256,
            max_pending_connections: 2048,
            max_connections: 25_000,
            shutdown_timeout: 10,
            max_alloc_size: None,
        }
    }
}

/// Controls traffic steering.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Routing {
    /// Accept and forward unknown Envelope items to the upstream.
    ///
    /// Forwarding unknown items should be enabled in most cases to allow proxying traffic for newer
    /// SDK versions. The upstream in Sentry makes the final decision on which items are valid. If
    /// this is disabled, just the unknown items are removed from Envelopes, and the rest is
    /// processed as usual.
    ///
    /// Defaults to `true` for all Relay modes other than processing mode. In processing mode, this
    /// is disabled by default since the item cannot be handled.
    accept_unknown_items: Option<bool>,
}

/// Http content encoding for both incoming and outgoing web requests.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HttpEncoding {
    /// Identity function without no compression.
    ///
    /// This is the default encoding and does not require the presence of the `content-encoding`
    /// HTTP header.
    Identity,
    /// Compression using a [zlib](https://en.wikipedia.org/wiki/Zlib) structure with
    /// [deflate](https://en.wikipedia.org/wiki/DEFLATE) encoding.
    ///
    /// These structures are defined in [RFC 1950](https://datatracker.ietf.org/doc/html/rfc1950)
    /// and [RFC 1951](https://datatracker.ietf.org/doc/html/rfc1951).
    Deflate,
    /// A format using the [Lempel-Ziv coding](https://en.wikipedia.org/wiki/LZ77_and_LZ78#LZ77)
    /// (LZ77), with a 32-bit CRC.
    ///
    /// This is the original format of the UNIX gzip program. The HTTP/1.1 standard also recommends
    /// that the servers supporting this content-encoding should recognize `x-gzip` as an alias, for
    /// compatibility purposes.
    Gzip,
    /// A format using the [Brotli](https://en.wikipedia.org/wiki/Brotli) algorithm.
    Br,
}

impl HttpEncoding {
    /// Parses a [`HttpEncoding`] from its `content-encoding` header value.
    pub fn parse(str: &str) -> Self {
        let str = str.trim();
        if str.eq_ignore_ascii_case("br") {
            Self::Br
        } else if str.eq_ignore_ascii_case("gzip") || str.eq_ignore_ascii_case("x-gzip") {
            Self::Gzip
        } else if str.eq_ignore_ascii_case("deflate") {
            Self::Deflate
        } else {
            Self::Identity
        }
    }

    /// Returns the value for the `content-encoding` HTTP header.
    ///
    /// Returns `None` for [`Identity`](Self::Identity), and `Some` for other encodings.
    pub fn name(&self) -> Option<&'static str> {
        match self {
            Self::Identity => None,
            Self::Deflate => Some("deflate"),
            Self::Gzip => Some("gzip"),
            Self::Br => Some("br"),
        }
    }
}

impl Default for HttpEncoding {
    fn default() -> Self {
        Self::Identity
    }
}

/// Controls authentication with upstream.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Http {
    /// Timeout for upstream requests in seconds.
    ///
    /// This timeout covers the time from sending the request until receiving response headers.
    /// Neither the connection process and handshakes, nor reading the response body is covered in
    /// this timeout.
    timeout: u32,
    /// Timeout for establishing connections with the upstream in seconds.
    ///
    /// This includes SSL handshakes. Relay reuses connections when the upstream supports connection
    /// keep-alive. Connections are retained for a maximum 75 seconds, or 15 seconds of inactivity.
    connection_timeout: u32,
    /// Maximum interval between failed request retries in seconds.
    max_retry_interval: u32,
    /// The custom HTTP Host header to send to the upstream.
    host_header: Option<String>,
    /// The interval in seconds at which Relay attempts to reauthenticate with the upstream server.
    ///
    /// Re-authentication happens even when Relay is idle. If authentication fails, Relay reverts
    /// back into startup mode and tries to establish a connection. During this time, incoming
    /// envelopes will be buffered.
    ///
    /// Defaults to `600` (10 minutes).
    auth_interval: Option<u64>,
    /// The maximum time of experiencing uninterrupted network failures until Relay considers that
    /// it has encountered a network outage in seconds.
    ///
    /// During a network outage relay will try to reconnect and will buffer all upstream messages
    /// until it manages to reconnect.
    outage_grace_period: u64,
    /// Content encoding to apply to upstream store requests.
    ///
    /// By default, Relay applies `gzip` content encoding to compress upstream requests. Compression
    /// can be disabled to reduce CPU consumption, but at the expense of increased network traffic.
    ///
    /// This setting applies to all store requests of SDK data, including events, transactions,
    /// envelopes and sessions. At the moment, this does not apply to Relay's internal queries.
    ///
    /// Available options are:
    ///
    ///  - `identity`: Disables compression.
    ///  - `deflate`: Compression using a zlib header with deflate encoding.
    ///  - `gzip` (default): Compression using gzip.
    ///  - `br`: Compression using the brotli algorithm.
    encoding: HttpEncoding,
}

impl Default for Http {
    fn default() -> Self {
        Http {
            timeout: 5,
            connection_timeout: 3,
            max_retry_interval: 60, // 1 minute
            host_header: None,
            auth_interval: Some(600), // 10 minutes
            outage_grace_period: DEFAULT_NETWORK_OUTAGE_GRACE_PERIOD,
            encoding: HttpEncoding::Gzip,
        }
    }
}

/// Controls internal caching behavior.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Cache {
    /// The cache timeout for project configurations in seconds.
    project_expiry: u32,
    /// Continue using project state this many seconds after cache expiry while a new state is
    /// being fetched. This is added on top of `project_expiry` and `miss_expiry`. Default is 0.
    project_grace_period: u32,
    /// The cache timeout for downstream relay info (public keys) in seconds.
    relay_expiry: u32,
    /// The cache timeout for envelopes (store) before dropping them.
    #[serde(alias = "event_expiry")]
    envelope_expiry: u32,
    /// The maximum amount of envelopes to queue before dropping them.
    #[serde(alias = "event_buffer_size")]
    envelope_buffer_size: u32,
    /// The cache timeout for non-existing entries.
    miss_expiry: u32,
    /// The buffer timeout for batched queries before sending them upstream in ms.
    batch_interval: u32,
    /// The maximum number of project configs to fetch from Sentry at once. Defaults to 500.
    ///
    /// `cache.batch_interval` controls how quickly batches are sent, this controls the batch size.
    batch_size: usize,
    /// Interval for watching local cache override files in seconds.
    file_interval: u32,
    /// Interval for evicting outdated project configs from memory.
    eviction_interval: u32,
}

impl Default for Cache {
    fn default() -> Self {
        Cache {
            project_expiry: 300, // 5 minutes
            project_grace_period: 0,
            relay_expiry: 3600,   // 1 hour
            envelope_expiry: 600, // 10 minutes
            envelope_buffer_size: 1000,
            miss_expiry: 60,     // 1 minute
            batch_interval: 100, // 100ms
            batch_size: 500,
            file_interval: 10,     // 10 seconds
            eviction_interval: 60, // 60 seconds
        }
    }
}

/// Define the topics over which Relay communicates with Sentry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaTopic {
    /// Simple events (without attachments) topic.
    Events,
    /// Complex events (with attachments) topic.
    Attachments,
    /// Transaction events topic.
    Transactions,
    /// Shared outcomes topic for Relay and Sentry.
    Outcomes,
    /// Override for billing critical outcomes.
    OutcomesBilling,
    /// Session health updates.
    Sessions,
    /// Aggregate Metrics.
    Metrics,
    /// Profiles
    Profiles,
    /// ReplayPayloads, large blobs sent by the replay sdk
    ReplayPayloads,
}

/// Configuration for topics.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct TopicAssignments {
    /// Simple events topic name.
    pub events: TopicAssignment,
    /// Events with attachments topic name.
    pub attachments: TopicAssignment,
    /// Transaction events topic name.
    pub transactions: TopicAssignment,
    /// Outcomes topic name.
    pub outcomes: TopicAssignment,
    /// Outcomes topic name for billing critical outcomes. Defaults to the assignment of `outcomes`.
    pub outcomes_billing: Option<TopicAssignment>,
    /// Session health topic name.
    pub sessions: TopicAssignment,
    /// Metrics topic name.
    pub metrics: TopicAssignment,
    /// Stacktrace topic name
    pub profiles: TopicAssignment,
    /// Payloads topic name.
    pub replay_payloads: TopicAssignment,
}

impl TopicAssignments {
    /// Get a topic assignment by KafkaTopic value
    pub fn get(&self, kafka_topic: KafkaTopic) -> &TopicAssignment {
        match kafka_topic {
            KafkaTopic::Attachments => &self.attachments,
            KafkaTopic::Events => &self.events,
            KafkaTopic::Transactions => &self.transactions,
            KafkaTopic::Outcomes => &self.outcomes,
            KafkaTopic::OutcomesBilling => self.outcomes_billing.as_ref().unwrap_or(&self.outcomes),
            KafkaTopic::Sessions => &self.sessions,
            KafkaTopic::Metrics => &self.metrics,
            KafkaTopic::Profiles => &self.profiles,
            KafkaTopic::ReplayPayloads => &self.replay_payloads,
        }
    }
}

impl Default for TopicAssignments {
    fn default() -> Self {
        Self {
            events: "ingest-events".to_owned().into(),
            attachments: "ingest-attachments".to_owned().into(),
            transactions: "ingest-transactions".to_owned().into(),
            outcomes: "outcomes".to_owned().into(),
            outcomes_billing: None,
            sessions: "ingest-sessions".to_owned().into(),
            metrics: "ingest-metrics".to_owned().into(),
            profiles: "profiles".to_owned().into(),
            replay_payloads: "ingest-replay-payloads".to_owned().into(),
        }
    }
}

/// Configuration for a "logical" topic/datasink that Relay should forward data into.
///
/// Can be either a string containing the kafka topic name to produce into (using the default
/// `kafka_config`), or an object containing keys `topic_name` and `kafka_config_name` for using a
/// custom kafka cluster.
///
/// See documentation for `secondary_kafka_configs` for more information.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum TopicAssignment {
    /// String containing the kafka topic name. In this case the default kafka cluster configured
    /// in `kafka_config` will be used.
    Primary(String),
    /// Object containing topic name and string identifier of one of the clusters configured in
    /// `secondary_kafka_configs`. In this case that custom kafkaconfig will be used to produce
    /// data to the given topic name.
    Secondary {
        /// The topic name to use.
        #[serde(rename = "name")]
        topic_name: String,
        /// An identifier referencing one of the kafka configurations in `secondary_kafka_configs`.
        #[serde(rename = "config")]
        kafka_config_name: String,
    },
}

impl From<String> for TopicAssignment {
    fn from(topic_name: String) -> TopicAssignment {
        TopicAssignment::Primary(topic_name)
    }
}

impl TopicAssignment {
    /// Get the topic name from this topic assignment.
    fn topic_name(&self) -> &str {
        match *self {
            TopicAssignment::Primary(ref s) => s.as_str(),
            TopicAssignment::Secondary { ref topic_name, .. } => topic_name.as_str(),
        }
    }

    /// Get the name of the kafka config to use. `None` means default configuration.
    fn kafka_config_name(&self) -> Option<&str> {
        match *self {
            TopicAssignment::Primary(_) => None,
            TopicAssignment::Secondary {
                ref kafka_config_name,
                ..
            } => Some(kafka_config_name.as_str()),
        }
    }
}

/// A name value pair of Kafka config parameter.
#[derive(Serialize, Deserialize, Debug)]
pub struct KafkaConfigParam {
    /// Name of the Kafka config parameter.
    pub name: String,
    /// Value of the Kafka config parameter.
    pub value: String,
}

fn default_max_secs_in_future() -> u32 {
    60 // 1 minute
}

fn default_max_secs_in_past() -> u32 {
    30 * 24 * 3600 // 30 days
}

fn default_max_session_secs_in_past() -> u32 {
    5 * 24 * 3600 // 5 days
}

fn default_chunk_size() -> ByteSize {
    ByteSize::mebibytes(1)
}

fn default_projectconfig_cache_prefix() -> String {
    "relayconfig".to_owned()
}

#[allow(clippy::unnecessary_wraps)]
fn default_max_rate_limit() -> Option<u32> {
    Some(300) // 5 minutes
}

/// Controls Sentry-internal event processing.
#[derive(Serialize, Deserialize, Debug)]
pub struct Processing {
    /// True if the Relay should do processing. Defaults to `false`.
    pub enabled: bool,
    /// GeoIp DB file source.
    #[serde(default)]
    pub geoip_path: Option<PathBuf>,
    /// Maximum future timestamp of ingested events.
    #[serde(default = "default_max_secs_in_future")]
    pub max_secs_in_future: u32,
    /// Maximum age of ingested events. Older events will be adjusted to `now()`.
    #[serde(default = "default_max_secs_in_past")]
    pub max_secs_in_past: u32,
    /// Maximum age of ingested sessions. Older sessions will be dropped.
    #[serde(default = "default_max_session_secs_in_past")]
    pub max_session_secs_in_past: u32,
    /// Kafka producer configurations.
    pub kafka_config: Vec<KafkaConfigParam>,
    /// Additional kafka producer configurations.
    ///
    /// The `kafka_config` is the default producer configuration used for all topics. A secondary
    /// kafka config can be referenced in `topics:` like this:
    ///
    /// ```yaml
    /// secondary_kafka_configs:
    ///   mycustomcluster:
    ///     - name: 'bootstrap.servers'
    ///       value: 'sentry_kafka_metrics:9093'
    ///
    /// topics:
    ///   transactions: ingest-transactions
    ///   metrics:
    ///     name: ingest-metrics
    ///     config: mycustomcluster
    /// ```
    ///
    /// Then metrics will be produced to an entirely different Kafka cluster.
    #[serde(default)]
    pub secondary_kafka_configs: BTreeMap<String, Vec<KafkaConfigParam>>,
    /// Kafka topic names.
    #[serde(default)]
    pub topics: TopicAssignments,
    /// Redis hosts to connect to for storing state for rate limits.
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    /// Maximum chunk size of attachments for Kafka.
    #[serde(default = "default_chunk_size")]
    pub attachment_chunk_size: ByteSize,
    /// Prefix to use when looking up project configs in Redis. Defaults to "relayconfig".
    #[serde(default = "default_projectconfig_cache_prefix")]
    pub projectconfig_cache_prefix: String,
    /// Maximum rate limit to report to clients.
    #[serde(default = "default_max_rate_limit")]
    pub max_rate_limit: Option<u32>,
}

impl Default for Processing {
    /// Constructs a disabled processing configuration.
    fn default() -> Self {
        Self {
            enabled: false,
            geoip_path: None,
            max_secs_in_future: default_max_secs_in_future(),
            max_secs_in_past: default_max_secs_in_past(),
            max_session_secs_in_past: default_max_session_secs_in_past(),
            kafka_config: Vec::new(),
            secondary_kafka_configs: BTreeMap::new(),
            topics: TopicAssignments::default(),
            redis: None,
            attachment_chunk_size: default_chunk_size(),
            projectconfig_cache_prefix: default_projectconfig_cache_prefix(),
            max_rate_limit: default_max_rate_limit(),
        }
    }
}

/// Configuration values for the outcome aggregator
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct OutcomeAggregatorConfig {
    /// Defines the width of the buckets into which outcomes are aggregated, in seconds.
    pub bucket_interval: u64,
    /// Defines how often all buckets are flushed, in seconds.
    pub flush_interval: u64,
}

impl Default for OutcomeAggregatorConfig {
    fn default() -> Self {
        Self {
            bucket_interval: 60,
            flush_interval: 120,
        }
    }
}

/// Determines how to emit outcomes.
/// For compatibility reasons, this can either be true, false or AsClientReports
#[derive(Copy, Clone, Debug, PartialEq, Eq)]

pub enum EmitOutcomes {
    /// Do not emit any outcomes
    None,
    /// Emit outcomes as client reports
    AsClientReports,
    /// Emit outcomes as outcomes
    AsOutcomes,
}

impl EmitOutcomes {
    /// Returns true of outcomes are emitted via http, kafka, or client reports.
    pub fn any(&self) -> bool {
        !matches!(self, EmitOutcomes::None)
    }
}

impl Serialize for EmitOutcomes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // For compatibility, serialize None and AsOutcomes as booleans.
        match self {
            Self::None => serializer.serialize_bool(false),
            Self::AsClientReports => serializer.serialize_str("as_client_reports"),
            Self::AsOutcomes => serializer.serialize_bool(true),
        }
    }
}

struct EmitOutcomesVisitor;

impl<'de> Visitor<'de> for EmitOutcomesVisitor {
    type Value = EmitOutcomes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("true, false, or 'as_client_reports'")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(if v {
            EmitOutcomes::AsOutcomes
        } else {
            EmitOutcomes::None
        })
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v == "as_client_reports" {
            Ok(EmitOutcomes::AsClientReports)
        } else {
            Err(E::invalid_value(Unexpected::Str(v), &"as_client_reports"))
        }
    }
}

impl<'de> Deserialize<'de> for EmitOutcomes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(EmitOutcomesVisitor)
    }
}

/// Outcome generation specific configuration values.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Outcomes {
    /// Controls whether outcomes will be emitted when processing is disabled.
    /// Processing relays always emit outcomes (for backwards compatibility).
    /// Can take the following values: false, "as_client_reports", true
    pub emit_outcomes: EmitOutcomes,
    /// Controls wheather client reported outcomes should be emitted.
    pub emit_client_outcomes: bool,
    /// The maximum number of outcomes that are batched before being sent
    /// via http to the upstream (only applies to non processing relays).
    pub batch_size: usize,
    /// The maximum time interval (in milliseconds) that an outcome may be batched
    /// via http to the upstream (only applies to non processing relays).
    pub batch_interval: u64,
    /// Defines the source string registered in the outcomes originating from
    /// this Relay (typically something like the region or the layer).
    pub source: Option<String>,
    /// Configures the outcome aggregator.
    pub aggregator: OutcomeAggregatorConfig,
}

impl Default for Outcomes {
    fn default() -> Self {
        Outcomes {
            emit_outcomes: EmitOutcomes::AsClientReports,
            emit_client_outcomes: true,
            batch_size: 1000,
            batch_interval: 500,
            source: None,
            aggregator: OutcomeAggregatorConfig::default(),
        }
    }
}

/// Minimal version of a config for dumping out.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MinimalConfig {
    /// The relay part of the config.
    pub relay: Relay,
}

impl MinimalConfig {
    /// Saves the config in the given config folder as config.yml
    pub fn save_in_folder<P: AsRef<Path>>(&self, p: P) -> Result<(), ConfigError> {
        let path = p.as_ref();
        if fs::metadata(path).is_err() {
            fs::create_dir_all(path)
                .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotOpenFile).file(path))?;
        }
        self.save(path)
    }
}

impl ConfigObject for MinimalConfig {
    fn format() -> ConfigFormat {
        ConfigFormat::Yaml
    }

    fn name() -> &'static str {
        "config"
    }
}

/// Alternative serialization of RelayInfo for config file using snake case.
mod config_relay_info {
    use super::*;

    use serde::ser::SerializeMap;

    // Uses snake_case as opposed to camelCase.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct RelayInfoConfig {
        public_key: PublicKey,
        #[serde(default)]
        internal: bool,
    }

    impl From<RelayInfoConfig> for RelayInfo {
        fn from(v: RelayInfoConfig) -> Self {
            RelayInfo {
                public_key: v.public_key,
                internal: v.internal,
            }
        }
    }

    impl From<RelayInfo> for RelayInfoConfig {
        fn from(v: RelayInfo) -> Self {
            RelayInfoConfig {
                public_key: v.public_key,
                internal: v.internal,
            }
        }
    }

    pub(super) fn deserialize<'de, D>(des: D) -> Result<HashMap<RelayId, RelayInfo>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = HashMap::<RelayId, RelayInfoConfig>::deserialize(des)?;
        Ok(map.into_iter().map(|(k, v)| (k, v.into())).collect())
    }

    pub(super) fn serialize<S>(elm: &HashMap<RelayId, RelayInfo>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = ser.serialize_map(Some(elm.len()))?;

        for (k, v) in elm {
            map.serialize_entry(k, &RelayInfoConfig::from(v.clone()))?;
        }

        map.end()
    }
}

/// Authentication options.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AuthConfig {
    /// Controls responses from the readiness health check endpoint based on authentication.
    #[serde(default, skip_serializing_if = "is_default")]
    pub ready: ReadinessCondition,

    /// Statically authenticated downstream relays.
    #[serde(default, with = "config_relay_info")]
    pub static_relays: HashMap<RelayId, RelayInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ConfigValues {
    #[serde(default)]
    relay: Relay,
    #[serde(default)]
    http: Http,
    #[serde(default)]
    cache: Cache,
    #[serde(default)]
    limits: Limits,
    #[serde(default)]
    logging: relay_log::LogConfig,
    #[serde(default)]
    routing: Routing,
    #[serde(default)]
    metrics: Metrics,
    #[serde(default)]
    sentry: relay_log::SentryConfig,
    #[serde(default)]
    processing: Processing,
    #[serde(default)]
    outcomes: Outcomes,
    #[serde(default)]
    aggregator: AggregatorConfig,
    #[serde(default)]
    auth: AuthConfig,
}

impl ConfigObject for ConfigValues {
    fn format() -> ConfigFormat {
        ConfigFormat::Yaml
    }

    fn name() -> &'static str {
        "config"
    }
}

/// Config struct.
pub struct Config {
    values: ConfigValues,
    credentials: Option<Credentials>,
    path: PathBuf,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("path", &self.path)
            .field("values", &self.values)
            .finish()
    }
}

impl Config {
    /// Loads a config from a given config folder.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let path = env::current_dir()
            .map(|x| x.join(path.as_ref()))
            .unwrap_or_else(|_| path.as_ref().to_path_buf());

        let config = Config {
            values: ConfigValues::load(&path)?,
            credentials: if Credentials::path(&path).exists() {
                Some(Credentials::load(&path)?)
            } else {
                None
            },
            path: path.clone(),
        };

        if cfg!(not(feature = "processing")) && config.processing_enabled() {
            return Err(ConfigError::new(ConfigErrorKind::ProcessingNotAvailable).file(&path));
        }

        Ok(config)
    }

    /// Creates a config from a JSON value.
    ///
    /// This is mostly useful for tests.
    pub fn from_json_value(value: serde_json::Value) -> Result<Config, ConfigError> {
        Ok(Config {
            values: serde_json::from_value(value)
                .map_err(|err| ConfigError::wrap(err, ConfigErrorKind::BadJson))?,
            credentials: None,
            path: PathBuf::new(),
        })
    }

    /// Override configuration with values coming from other sources (e.g. env variables or
    /// command line parameters)
    pub fn apply_override(
        &mut self,
        mut overrides: OverridableConfig,
    ) -> Result<&mut Self, ConfigError> {
        let relay = &mut self.values.relay;

        if let Some(mode) = overrides.mode {
            relay.mode = mode
                .parse::<RelayMode>()
                .map_err(|err| ConfigError::for_field(err, "mode"))?;
        }

        if let Some(upstream) = overrides.upstream {
            relay.upstream = upstream
                .parse::<UpstreamDescriptor>()
                .map_err(|err| ConfigError::for_field(err, "upstream"))?;
        }

        if let Some(host) = overrides.host {
            relay.host = host
                .parse::<IpAddr>()
                .map_err(|err| ConfigError::for_field(err, "host"))?;
        }

        if let Some(port) = overrides.port {
            relay.port = port
                .as_str()
                .parse()
                .map_err(|err| ConfigError::for_field(err, "port"))?;
        }

        let processing = &mut self.values.processing;
        if let Some(enabled) = overrides.processing {
            match enabled.to_lowercase().as_str() {
                "true" | "1" => processing.enabled = true,
                "false" | "0" | "" => processing.enabled = false,
                _ => {
                    return Err(ConfigError::new(ConfigErrorKind::InvalidValue).field("processing"));
                }
            }
        }

        if let Some(redis) = overrides.redis_url {
            processing.redis = Some(RedisConfig::Single(redis))
        }

        if let Some(kafka_url) = overrides.kafka_url {
            let existing = processing
                .kafka_config
                .iter_mut()
                .find(|e| e.name == "bootstrap.servers");

            if let Some(config_param) = existing {
                config_param.value = kafka_url;
            } else {
                processing.kafka_config.push(KafkaConfigParam {
                    name: "bootstrap.servers".to_owned(),
                    value: kafka_url,
                })
            }
        }
        // credentials overrides
        let id = if let Some(id) = overrides.id {
            let id = Uuid::parse_str(&id).map_err(|err| ConfigError::for_field(err, "id"))?;
            Some(id)
        } else {
            None
        };
        let public_key = if let Some(public_key) = overrides.public_key {
            let public_key = public_key
                .parse::<PublicKey>()
                .map_err(|err| ConfigError::for_field(err, "public_key"))?;
            Some(public_key)
        } else {
            None
        };

        let secret_key = if let Some(secret_key) = overrides.secret_key {
            let secret_key = secret_key
                .parse::<SecretKey>()
                .map_err(|err| ConfigError::for_field(err, "secret_key"))?;
            Some(secret_key)
        } else {
            None
        };
        let mut outcomes = &mut self.values.outcomes;
        if overrides.outcome_source.is_some() {
            outcomes.source = overrides.outcome_source.take();
        }

        if let Some(credentials) = &mut self.credentials {
            //we have existing credentials we may override some entries
            if let Some(id) = id {
                credentials.id = id;
            }
            if let Some(public_key) = public_key {
                credentials.public_key = public_key;
            }
            if let Some(secret_key) = secret_key {
                credentials.secret_key = secret_key
            }
        } else {
            //no existing credentials we may only create the full credentials
            match (id, public_key, secret_key) {
                (Some(id), Some(public_key), Some(secret_key)) => {
                    self.credentials = Some(Credentials {
                        secret_key,
                        public_key,
                        id,
                    })
                }
                (None, None, None) => {
                    // nothing provided, we'll just leave the credentials None, maybe we
                    // don't need them in the current command or we'll override them later
                }
                _ => {
                    return Err(ConfigError::new(ConfigErrorKind::InvalidValue)
                        .field("incomplete credentials"));
                }
            }
        }

        let limits = &mut self.values.limits;
        if let Some(shutdown_timeout) = overrides.shutdown_timeout {
            if let Ok(shutdown_timeout) = shutdown_timeout.parse::<u64>() {
                limits.shutdown_timeout = shutdown_timeout;
            }
        }

        Ok(self)
    }

    /// Checks if the config is already initialized.
    pub fn config_exists<P: AsRef<Path>>(path: P) -> bool {
        fs::metadata(ConfigValues::path(path.as_ref())).is_ok()
    }

    /// Returns the filename of the config file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Dumps out a YAML string of the values.
    pub fn to_yaml_string(&self) -> Result<String, ConfigError> {
        serde_yaml::to_string(&self.values)
            .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::CouldNotWriteFile))
    }

    /// Regenerates the relay credentials.
    ///
    /// This also writes the credentials back to the file.
    pub fn regenerate_credentials(&mut self) -> Result<(), ConfigError> {
        let creds = Credentials::generate();
        creds.save(&self.path)?;
        self.credentials = Some(creds);
        Ok(())
    }

    /// Return the current credentials
    pub fn credentials(&self) -> Option<&Credentials> {
        self.credentials.as_ref()
    }

    /// Set new credentials.
    ///
    /// This also writes the credentials back to the file.
    pub fn replace_credentials(
        &mut self,
        credentials: Option<Credentials>,
    ) -> Result<bool, ConfigError> {
        if self.credentials == credentials {
            return Ok(false);
        }

        match credentials {
            Some(ref creds) => {
                creds.save(&self.path)?;
            }
            None => {
                let path = Credentials::path(&self.path);
                if fs::metadata(&path).is_ok() {
                    fs::remove_file(&path).map_err(|e| {
                        ConfigError::wrap(e, ConfigErrorKind::CouldNotWriteFile).file(&path)
                    })?;
                }
            }
        }

        self.credentials = credentials;
        Ok(true)
    }

    /// Returns `true` if the config is ready to use.
    pub fn has_credentials(&self) -> bool {
        self.credentials.is_some()
    }

    /// Returns the secret key if set.
    pub fn secret_key(&self) -> Option<&SecretKey> {
        self.credentials.as_ref().map(|x| &x.secret_key)
    }

    /// Returns the public key if set.
    pub fn public_key(&self) -> Option<&PublicKey> {
        self.credentials.as_ref().map(|x| &x.public_key)
    }

    /// Returns the relay ID.
    pub fn relay_id(&self) -> Option<&RelayId> {
        self.credentials.as_ref().map(|x| &x.id)
    }

    /// Returns the relay mode.
    pub fn relay_mode(&self) -> RelayMode {
        self.values.relay.mode
    }

    /// Returns the upstream target as descriptor.
    pub fn upstream_descriptor(&self) -> &UpstreamDescriptor<'_> {
        &self.values.relay.upstream
    }

    /// Returns the custom HTTP "Host" header.
    pub fn http_host_header(&self) -> Option<&str> {
        self.values.http.host_header.as_deref()
    }

    /// Returns the listen address.
    pub fn listen_addr(&self) -> SocketAddr {
        (self.values.relay.host, self.values.relay.port).into()
    }

    /// Returns the TLS listen address.
    pub fn tls_listen_addr(&self) -> Option<SocketAddr> {
        if self.values.relay.tls_identity_path.is_some() {
            let port = self.values.relay.tls_port.unwrap_or(3443);
            Some((self.values.relay.host, port).into())
        } else {
            None
        }
    }

    /// Returns the path to the identity bundle
    pub fn tls_identity_path(&self) -> Option<&Path> {
        self.values.relay.tls_identity_path.as_deref()
    }

    /// Returns the password for the identity bundle
    pub fn tls_identity_password(&self) -> Option<&str> {
        self.values.relay.tls_identity_password.as_deref()
    }

    /// Returns `true` when project IDs should be overriden rather than validated.
    ///
    /// Defaults to `false`, which requires project ID validation.
    pub fn override_project_ids(&self) -> bool {
        self.values.relay.override_project_ids
    }

    /// Returns `true` if Relay requires authentication for readiness.
    ///
    /// See [`ReadinessCondition`] for more information.
    pub fn requires_auth(&self) -> bool {
        match self.values.auth.ready {
            ReadinessCondition::Authenticated => self.relay_mode() == RelayMode::Managed,
            ReadinessCondition::Always => false,
        }
    }

    /// Returns the interval at which Realy should try to re-authenticate with the upstream.
    ///
    /// Always disabled in processing mode.
    pub fn http_auth_interval(&self) -> Option<Duration> {
        if self.processing_enabled() {
            return None;
        }

        match self.values.http.auth_interval {
            None | Some(0) => None,
            Some(secs) => Some(Duration::from_secs(secs)),
        }
    }

    /// The maximum time of experiencing uninterrupted network failures until Relay considers that
    /// it has encountered a network outage.
    pub fn http_outage_grace_period(&self) -> Duration {
        Duration::from_secs(self.values.http.outage_grace_period)
    }

    /// Content encoding of upstream requests.
    pub fn http_encoding(&self) -> HttpEncoding {
        self.values.http.encoding
    }

    /// Returns whether this Relay should emit outcomes.
    ///
    /// This is `true` either if `outcomes.emit_outcomes` is explicitly enabled, or if this Relay is
    /// in processing mode.
    pub fn emit_outcomes(&self) -> EmitOutcomes {
        if self.processing_enabled() {
            return EmitOutcomes::AsOutcomes;
        }
        self.values.outcomes.emit_outcomes
    }

    /// Returns whether this Relay should emit client outcomes
    ///
    /// Relays that do not emit client outcomes will forward client recieved outcomes
    /// directly to the next relay in the chain as client report envelope.  This is only done
    /// if this relay emits outcomes at all. A relay that will not emit outcomes
    /// will forward the envelope unchanged.
    ///
    /// This flag can be explicitly disabled on processing relays as well to prevent the
    /// emitting of client outcomes to the kafka topic.
    pub fn emit_client_outcomes(&self) -> bool {
        self.values.outcomes.emit_client_outcomes
    }

    /// Returns the maximum number of outcomes that are batched before being sent
    pub fn outcome_batch_size(&self) -> usize {
        self.values.outcomes.batch_size
    }

    /// Returns the maximum interval that an outcome may be batched
    pub fn outcome_batch_interval(&self) -> Duration {
        Duration::from_millis(self.values.outcomes.batch_interval)
    }

    /// The originating source of the outcome
    pub fn outcome_source(&self) -> Option<&str> {
        self.values.outcomes.source.as_deref()
    }

    /// Returns the width of the buckets into which outcomes are aggregated, in seconds.
    pub fn outcome_aggregator(&self) -> &OutcomeAggregatorConfig {
        &self.values.outcomes.aggregator
    }

    /// Returns logging configuration.
    pub fn logging(&self) -> &relay_log::LogConfig {
        &self.values.logging
    }

    /// Returns logging configuration.
    pub fn sentry(&self) -> &relay_log::SentryConfig {
        &self.values.sentry
    }

    /// The maximum allocation size relay can make.
    pub fn max_alloc_size(&self) -> Option<usize> {
        self.values
            .limits
            .max_alloc_size
            .as_ref()
            .map(ByteSize::as_bytes)
    }

    /// Returns the socket addresses for statsd.
    ///
    /// If stats is disabled an empty vector is returned.
    pub fn statsd_addrs(&self) -> Result<Vec<SocketAddr>, ConfigError> {
        if let Some(ref addr) = self.values.metrics.statsd {
            let addrs = addr
                .as_str()
                .to_socket_addrs()
                .map_err(|e| ConfigError::wrap(e, ConfigErrorKind::InvalidValue).file(&self.path))?
                .collect();
            Ok(addrs)
        } else {
            Ok(vec![])
        }
    }

    /// Return the prefix for statsd metrics.
    pub fn metrics_prefix(&self) -> &str {
        &self.values.metrics.prefix
    }

    /// Returns the default tags for statsd metrics.
    pub fn metrics_default_tags(&self) -> &BTreeMap<String, String> {
        &self.values.metrics.default_tags
    }

    /// Returns the name of the hostname tag that should be attached to each outgoing metric.
    pub fn metrics_hostname_tag(&self) -> Option<&str> {
        self.values.metrics.hostname_tag.as_deref()
    }

    /// Returns true if metrics buffering is enabled, false otherwise.
    pub fn metrics_buffering(&self) -> bool {
        self.values.metrics.buffering
    }

    /// Returns the global sample rate for all metrics.
    pub fn metrics_sample_rate(&self) -> f32 {
        self.values.metrics.sample_rate
    }

    /// Returns the default timeout for all upstream HTTP requests.
    pub fn http_timeout(&self) -> Duration {
        Duration::from_secs(self.values.http.timeout.into())
    }

    /// Returns the connection timeout for all upstream HTTP requests.
    pub fn http_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.values.http.connection_timeout.into())
    }

    /// Returns the failed upstream request retry interval.
    pub fn http_max_retry_interval(&self) -> Duration {
        Duration::from_secs(self.values.http.max_retry_interval.into())
    }

    /// Returns the expiry timeout for cached projects.
    pub fn project_cache_expiry(&self) -> Duration {
        Duration::from_secs(self.values.cache.project_expiry.into())
    }

    /// Returns the expiry timeout for cached relay infos (public keys).
    pub fn relay_cache_expiry(&self) -> Duration {
        Duration::from_secs(self.values.cache.relay_expiry.into())
    }

    /// Returns the timeout for buffered envelopes (due to upstream errors).
    pub fn envelope_buffer_expiry(&self) -> Duration {
        Duration::from_secs(self.values.cache.envelope_expiry.into())
    }

    /// Returns the maximum number of buffered envelopes
    pub fn envelope_buffer_size(&self) -> u32 {
        self.values.cache.envelope_buffer_size
    }

    /// Returns the expiry timeout for cached misses before trying to refetch.
    pub fn cache_miss_expiry(&self) -> Duration {
        Duration::from_secs(self.values.cache.miss_expiry.into())
    }

    /// Returns the grace period for project caches.
    pub fn project_grace_period(&self) -> Duration {
        Duration::from_secs(self.values.cache.project_grace_period.into())
    }

    /// Returns the number of seconds during which batchable queries are collected before sending
    /// them in a single request.
    pub fn query_batch_interval(&self) -> Duration {
        Duration::from_millis(self.values.cache.batch_interval.into())
    }

    /// Returns the interval in seconds in which local project configurations should be reloaded.
    pub fn local_cache_interval(&self) -> Duration {
        Duration::from_secs(self.values.cache.file_interval.into())
    }

    /// Returns the interval in seconds in which projects configurations should be freed from
    /// memory when expired.
    pub fn cache_eviction_interval(&self) -> Duration {
        Duration::from_secs(self.values.cache.eviction_interval.into())
    }

    /// Returns the maximum size of an event payload in bytes.
    pub fn max_event_size(&self) -> usize {
        self.values.limits.max_event_size.as_bytes()
    }

    /// Returns the maximum size of each attachment.
    pub fn max_attachment_size(&self) -> usize {
        self.values.limits.max_attachment_size.as_bytes()
    }

    /// Returns the maxmium combined size of attachments or payloads containing attachments
    /// (minidump, unreal, standalone attachments) in bytes.
    pub fn max_attachments_size(&self) -> usize {
        self.values.limits.max_attachments_size.as_bytes()
    }

    /// Returns the maxmium combined size of client reports in bytes.
    pub fn max_client_reports_size(&self) -> usize {
        self.values.limits.max_client_reports_size.as_bytes()
    }

    /// Returns the maximum size of an envelope payload in bytes.
    ///
    /// Individual item size limits still apply.
    pub fn max_envelope_size(&self) -> usize {
        self.values.limits.max_envelope_size.as_bytes()
    }

    /// Returns the maximum number of sessions per envelope.
    pub fn max_session_count(&self) -> usize {
        self.values.limits.max_session_count
    }

    /// Returns the maximum payload size for general API requests.
    pub fn max_api_payload_size(&self) -> usize {
        self.values.limits.max_api_payload_size.as_bytes()
    }

    /// Returns the maximum payload size for file uploads and chunks.
    pub fn max_api_file_upload_size(&self) -> usize {
        self.values.limits.max_api_file_upload_size.as_bytes()
    }

    /// Returns the maximum payload size for chunks
    pub fn max_api_chunk_upload_size(&self) -> usize {
        self.values.limits.max_api_chunk_upload_size.as_bytes()
    }

    /// Returns the maximum payload size for a profile
    pub fn max_profile_size(&self) -> usize {
        self.values.limits.max_profile_size.as_bytes()
    }

    /// Returns the maximum number of active requests
    pub fn max_concurrent_requests(&self) -> usize {
        self.values.limits.max_concurrent_requests
    }

    /// Returns the maximum number of active queries
    pub fn max_concurrent_queries(&self) -> usize {
        self.values.limits.max_concurrent_queries
    }

    /// The maximum number of seconds a query is allowed to take across retries.
    pub fn query_timeout(&self) -> Duration {
        Duration::from_secs(self.values.limits.query_timeout)
    }

    /// The maximum number of open connections to Relay.
    pub fn max_connections(&self) -> usize {
        self.values.limits.max_connections
    }

    /// The maximum number of connections to Relay that can be created at once.
    pub fn max_connection_rate(&self) -> usize {
        self.values.limits.max_connection_rate
    }

    /// The maximum number of pending connects to Relay.
    pub fn max_pending_connections(&self) -> i32 {
        self.values.limits.max_pending_connections
    }

    /// The maximum number of seconds to wait for pending envelopes after receiving a shutdown
    /// signal.
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.values.limits.shutdown_timeout)
    }

    /// Returns the number of cores to use for thread pools.
    pub fn cpu_concurrency(&self) -> usize {
        self.values.limits.max_thread_count
    }

    /// Returns the maximum size of a project config query.
    pub fn query_batch_size(&self) -> usize {
        self.values.cache.batch_size
    }

    /// Get filename for static project config.
    pub fn project_configs_path(&self) -> PathBuf {
        self.path.join("projects")
    }

    /// True if the Relay should do processing.
    pub fn processing_enabled(&self) -> bool {
        self.values.processing.enabled
    }

    /// The path to the GeoIp database required for event processing.
    pub fn geoip_path(&self) -> Option<&Path> {
        self.values.processing.geoip_path.as_deref()
    }

    /// Maximum future timestamp of ingested data.
    ///
    /// Events past this timestamp will be adjusted to `now()`. Sessions will be dropped.
    pub fn max_secs_in_future(&self) -> i64 {
        self.values.processing.max_secs_in_future.into()
    }

    /// Maximum age of ingested events. Older events will be adjusted to `now()`.
    pub fn max_secs_in_past(&self) -> i64 {
        self.values.processing.max_secs_in_past.into()
    }

    /// Maximum age of ingested sessions. Older sessions will be dropped.
    pub fn max_session_secs_in_past(&self) -> i64 {
        self.values.processing.max_session_secs_in_past.into()
    }

    /// Topic name and list of Kafka configuration parameters for a given topic.
    pub fn kafka_topic_name(&self, topic: KafkaTopic) -> &str {
        self.values.processing.topics.get(topic).topic_name()
    }

    /// Configuration name and list of Kafka configuration parameters for a given topic.
    pub fn kafka_config(
        &self,
        topic: KafkaTopic,
    ) -> Result<(Option<&str>, &[KafkaConfigParam]), ConfigErrorKind> {
        if let Some(config_name) = self.values.processing.topics.get(topic).kafka_config_name() {
            Ok((
                Some(config_name),
                self.values
                    .processing
                    .secondary_kafka_configs
                    .get(config_name)
                    .ok_or(ConfigErrorKind::UnknownKafkaConfigName)?
                    .as_slice(),
            ))
        } else {
            Ok((None, self.values.processing.kafka_config.as_slice()))
        }
    }

    /// Redis servers to connect to, for rate limiting.
    pub fn redis(&self) -> Option<&RedisConfig> {
        self.values.processing.redis.as_ref()
    }

    /// Chunk size of attachments in bytes.
    pub fn attachment_chunk_size(&self) -> usize {
        self.values.processing.attachment_chunk_size.as_bytes()
    }

    /// Default prefix to use when looking up project configs in Redis. This is only done when
    /// Relay is in processing mode.
    pub fn projectconfig_cache_prefix(&self) -> &str {
        &self.values.processing.projectconfig_cache_prefix
    }

    /// Maximum rate limit to report to clients in seconds.
    pub fn max_rate_limit(&self) -> Option<u64> {
        self.values.processing.max_rate_limit.map(u32::into)
    }

    /// Returns configuration for the metrics [aggregator](relay_metrics::Aggregator).
    pub fn aggregator_config(&self) -> AggregatorConfig {
        self.values.aggregator.clone()
    }

    /// Return the statically configured Relays.
    pub fn static_relays(&self) -> &HashMap<RelayId, RelayInfo> {
        &self.values.auth.static_relays
    }

    /// Returns `true` if unknown items should be accepted and forwarded.
    pub fn accept_unknown_items(&self) -> bool {
        let forward = self.values.routing.accept_unknown_items;
        forward.unwrap_or_else(|| !self.processing_enabled())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            values: ConfigValues::default(),
            credentials: None,
            path: PathBuf::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for renaming the envelope buffer flags.
    #[test]
    fn test_event_buffer_size() {
        let yaml = r###"
cache:
    event_buffer_size: 1000000
    event_expiry: 1800
"###;

        let values: ConfigValues = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(values.cache.envelope_buffer_size, 1_000_000);
        assert_eq!(values.cache.envelope_expiry, 1800);
    }

    #[test]
    fn test_emit_outcomes() {
        for (serialized, deserialized) in &[
            ("true", EmitOutcomes::AsOutcomes),
            ("false", EmitOutcomes::None),
            ("\"as_client_reports\"", EmitOutcomes::AsClientReports),
        ] {
            let value: EmitOutcomes = serde_json::from_str(serialized).unwrap();
            assert_eq!(value, *deserialized);
            assert_eq!(serde_json::to_string(&value).unwrap(), *serialized);
        }
    }

    #[test]
    fn test_emit_outcomes_invalid() {
        assert!(matches!(
            serde_json::from_str::<EmitOutcomes>("asdf"),
            Err(_)
        ));
    }
}
