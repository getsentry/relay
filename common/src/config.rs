use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use failure::{Backtrace, Context, Fail};
use log;
use marshal::processor::PiiConfig;
// Dsn must be imported from sentry and not sentry-types for compatibility with sentry::init!
use sentry_types::{Dsn, ProjectId};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;
use serde_yaml;

use auth::{generate_key_pair, generate_relay_id, PublicKey, RelayId, SecretKey};
use types::ByteSize;
use upstream::UpstreamDescriptor;

macro_rules! ctry {
    ($expr:expr, $kind:expr, $path:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ConfigError::new($path, err.context($kind))),
        }
    };
}

/// Indicates config related errors.
#[derive(Debug)]
pub struct ConfigError {
    filename: PathBuf,
    inner: Context<ConfigErrorKind>,
}

impl ConfigError {
    fn new<P: AsRef<Path>>(p: P, inner: Context<ConfigErrorKind>) -> Self {
        ConfigError {
            filename: p.as_ref().to_path_buf(),
            inner,
        }
    }

    /// Returns the filename that failed.
    pub fn filename(&self) -> &Path {
        &self.filename
    }

    /// Returns the error kind of the error.
    pub fn kind(&self) -> ConfigErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for ConfigError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} (file {})", self.inner, self.filename().display())
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
    /// Parsing/dumping YAML failed.
    #[fail(display = "could not parse yaml config file")]
    BadYaml,
    /// Invalid config value
    #[fail(display = "invalid config value")]
    InvalidValue,
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

impl ConfigObject for Credentials {
    fn format() -> ConfigFormat {
        ConfigFormat::Json
    }
    fn name() -> &'static str {
        "credentials"
    }
}

/// Relay specific configuration values.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Relay {
    /// The upstream relay or sentry instance.
    pub upstream: UpstreamDescriptor<'static>,
    /// The host the relay should bind to (network interface)
    pub host: IpAddr,
    /// The port to bind for the unencrypted relay HTTP server.
    pub port: u16,
    /// Optional port to bind for the encrypted relay HTTPS server.
    pub tls_port: Option<u16>,
    /// The path to the identity (DER-encoded PKCS12) to use for TLS
    pub tls_identity_path: Option<PathBuf>,
    /// Password for the PKCS12 archive
    pub tls_identity_password: Option<String>,
}

impl Default for Relay {
    fn default() -> Self {
        Relay {
            upstream: "https://ingest.sentry.io/"
                .parse::<UpstreamDescriptor>()
                .unwrap(),
            host: "127.0.0.1".parse().unwrap(),
            port: 3000,
            tls_port: None,
            tls_identity_path: None,
            tls_identity_password: None,
        }
    }
}

/// Controls the log format
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Auto detect (pretty for tty, simplified for other)
    Auto,
    /// With colors
    Pretty,
    /// Simplified log output
    Simplified,
    /// Dump out JSON lines
    Json,
}

/// Controls the logging system.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Logging {
    /// The log level for the relay.
    level: log::LevelFilter,
    /// If set to true this emits log messages for failed event payloads.
    log_failed_payloads: bool,
    /// Controls the log format.
    format: LogFormat,
    /// When set to true, backtraces are forced on.
    enable_backtraces: bool,
}

impl Default for Logging {
    fn default() -> Self {
        Logging {
            level: log::LevelFilter::Info,
            log_failed_payloads: false,
            format: LogFormat::Auto,
            enable_backtraces: true,
        }
    }
}

/// Control the metrics.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Metrics {
    /// If set to a host/port string then metrics will be reported to this
    /// statsd instance.
    statsd: Option<String>,
    /// The prefix that should be added to all metrics.
    prefix: String,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            statsd: None,
            prefix: "sentry.relay".into(),
        }
    }
}

/// Controls various limits
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Limits {
    /// The maximum payload size for events.
    max_event_payload_size: ByteSize,
    /// The maximum payload size for general API requests.
    max_api_payload_size: ByteSize,
    /// The maximum payload size for file uploads and chunks.
    max_api_file_upload_size: ByteSize,
    /// The maximum payload size for chunks
    max_api_chunk_upload_size: ByteSize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_event_payload_size: ByteSize::from_kilobytes(256),
            max_api_payload_size: ByteSize::from_megabytes(20),
            max_api_file_upload_size: ByteSize::from_megabytes(40),
            max_api_chunk_upload_size: ByteSize::from_megabytes(100),
        }
    }
}

/// Controls authentication with upstream.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Http {
    /// Timeout for upstream requests in seconds.
    timeout: u32,
    /// Maximum interval between failed request retries in seconds.
    max_retry_interval: u32,
}

impl Default for Http {
    fn default() -> Self {
        Http {
            timeout: 5,
            max_retry_interval: 60,
        }
    }
}

/// Controls internal caching behavior.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Cache {
    /// The cache timeout for project configurations in seconds.
    project_expiry: u32,
    /// The cache timeout for downstream relay info (public keys) in seconds.
    relay_expiry: u32,
    /// The cache timeout for events (store) before dropping them.
    event_expiry: u32,
    /// The cache timeout for non-existing entries.
    miss_expiry: u32,
    /// The buffer timeout for batched queries before sending them upstream in ms.
    batch_interval: u32,
}

impl Default for Cache {
    fn default() -> Self {
        Cache {
            project_expiry: 300, // 5 minutes
            relay_expiry: 3600,  // 1 hour
            event_expiry: 600,   // 10 minutes
            miss_expiry: 60,     // 1 minute
            batch_interval: 100, // 100ms
        }
    }
}

/// Controls interal reporting to Sentry.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Sentry {
    dsn: Option<Dsn>,
    enabled: bool,
}

impl Default for Sentry {
    fn default() -> Self {
        Sentry {
            dsn: "https://0cc4a37e5aab4da58366266a87a95740@sentry.io/1269704"
                .parse()
                .ok(),
            enabled: false,
        }
    }
}

/// These are config values that the user can modify in the UI.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectConfig {
    /// URLs that are permitted for cross original JavaScript requests.
    pub allowed_domains: Vec<String>,
    /// List of relay public keys that are permitted to access this project.
    pub trusted_relays: Vec<PublicKey>,
    /// Configuration for PII stripping.
    pub pii_config: Option<PiiConfig>,
}

/// Project state that was hardcoded by the user in the local config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticProjectState {
    /// Indicates that the project is disabled.
    pub disabled: bool,
    /// A container of known public keys in the project.
    pub public_keys: HashMap<String, bool>,
    /// The project's slug if configured.
    pub slug: String,
    /// The project's current config.
    pub config: ProjectConfig,
}

/// Local overrides for project state
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct Projects {
    /// The overridden projects
    pub configs: HashMap<ProjectId, StaticProjectState>,
}

/// Controls interal reporting to Sentry.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct MinimalSentry {
    /// Set to true to enable sentry reporting to the default dsn.
    pub enabled: bool,
}

/// Minimal version of a config for dumping out.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MinimalConfig {
    /// The relay part of the config.
    pub relay: Relay,
    /// Turn on crash reporting?
    pub sentry: MinimalSentry,
}

impl MinimalConfig {
    /// Saves the config in the given config folder as config.yml
    pub fn save_in_folder<P: AsRef<Path>>(&self, p: P) -> Result<(), ConfigError> {
        if fs::metadata(p.as_ref()).is_err() {
            ctry!(
                fs::create_dir_all(p.as_ref()),
                ConfigErrorKind::CouldNotOpenFile,
                p.as_ref()
            );
        }
        self.save(p.as_ref())
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
    logging: Logging,
    #[serde(default)]
    metrics: Metrics,
    #[serde(default)]
    sentry: Sentry,
    #[serde(default)]
    projects: Projects,
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
        Ok(Config {
            values: ConfigValues::load(&path)?,
            credentials: if fs::metadata(Credentials::path(&path)).is_ok() {
                Some(Credentials::load(&path)?)
            } else {
                None
            },
            path,
        })
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
        Ok(ctry!(
            serde_yaml::to_string(&self.values),
            ConfigErrorKind::InvalidValue,
            &self.path
        ))
    }

    /// Regenerates the relay credentials.
    ///
    /// This also writes the credentials back to the file.
    pub fn regenerate_credentials(&mut self) -> Result<(), ConfigError> {
        info!("generating new relay credentials");
        let (sk, pk) = generate_key_pair();
        let creds = Credentials {
            secret_key: sk,
            public_key: pk,
            id: generate_relay_id(),
        };
        creds.save(&self.path)?;
        self.credentials = Some(creds);
        Ok(())
    }

    /// Return the current credentials
    pub fn credentials(&self) -> Option<&Credentials> {
        self.credentials.as_ref()
    }

    /// Set new credentials.
    pub fn replace_credentials(
        &mut self,
        credentials: Option<Credentials>,
    ) -> Result<bool, ConfigError> {
        if self.credentials == credentials {
            return Ok(false);
        }
        match credentials {
            Some(creds) => {
                creds.save(&self.path)?;
                self.credentials = Some(creds);
            }
            None => {
                let path = Credentials::path(&self.path);
                if fs::metadata(&path).is_ok() {
                    ctry!(
                        fs::remove_file(&path),
                        ConfigErrorKind::CouldNotWriteFile,
                        &path
                    );
                }
            }
        }
        Ok(true)
    }

    /// Returns `true` if the config is ready to use.
    pub fn has_credentials(&self) -> bool {
        self.credentials.is_some()
    }

    /// Returns the secret key if set.
    pub fn secret_key(&self) -> &SecretKey {
        &self.credentials.as_ref().unwrap().secret_key
    }

    /// Returns the public key if set.
    pub fn public_key(&self) -> &PublicKey {
        &self.credentials.as_ref().unwrap().public_key
    }

    /// Returns the relay ID.
    pub fn relay_id(&self) -> &RelayId {
        &self.credentials.as_ref().unwrap().id
    }

    /// Returns the upstream target as descriptor.
    pub fn upstream_descriptor(&self) -> &UpstreamDescriptor {
        &self.values.relay.upstream
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
        self.values
            .relay
            .tls_identity_path
            .as_ref()
            .map(|x| x.as_path())
    }

    /// Returns the password for the identity bundle
    pub fn tls_identity_password(&self) -> Option<&str> {
        self.values
            .relay
            .tls_identity_password
            .as_ref()
            .map(|x| &**x)
    }

    /// Returns the log level.
    pub fn log_level_filter(&self) -> log::LevelFilter {
        self.values.logging.level
    }

    /// Should backtraces be enabled?
    pub fn enable_backtraces(&self) -> bool {
        self.values.logging.enable_backtraces
    }

    /// Should we debug log bad payloads?
    pub fn log_failed_payloads(&self) -> bool {
        self.values.logging.log_failed_payloads
    }

    /// Which log format should be used?
    pub fn log_format(&self) -> LogFormat {
        self.values.logging.format
    }

    /// Returns the socket addresses for statsd.
    ///
    /// If stats is disabled an empty vector is returned.
    pub fn statsd_addrs(&self) -> Result<Vec<SocketAddr>, ConfigError> {
        if let Some(ref addr) = self.values.metrics.statsd {
            Ok(ctry!(
                addr.as_str().to_socket_addrs(),
                ConfigErrorKind::InvalidValue,
                &self.path
            ).collect())
        } else {
            Ok(vec![])
        }
    }

    /// Return the prefix for statsd metrics.
    pub fn metrics_prefix(&self) -> &str {
        &self.values.metrics.prefix
    }

    /// Returns the default timeout for all upstream HTTP requests.
    pub fn http_timeout(&self) -> Duration {
        Duration::from_secs(self.values.http.timeout.into())
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

    /// Returns the timeout for buffered events (due to upstream errors).
    pub fn event_buffer_expiry(&self) -> Duration {
        Duration::from_secs(self.values.cache.event_expiry.into())
    }

    /// Returns the expiry timeout for cached misses before trying to refetch.
    pub fn cache_miss_expiry(&self) -> Duration {
        Duration::from_secs(self.values.cache.miss_expiry.into())
    }

    /// Returns the number of seconds during which batchable queries are collected before sending
    /// them in a single request.
    pub fn query_batch_interval(&self) -> Duration {
        Duration::from_millis(self.values.cache.batch_interval.into())
    }

    /// Returns the maximum size of an event payload in bytes.
    pub fn max_event_payload_size(&self) -> usize {
        self.values.limits.max_event_payload_size.as_bytes() as usize
    }

    /// Returns the maximum payload size for general API requests.
    pub fn max_api_payload_size(&self) -> usize {
        self.values.limits.max_api_payload_size.as_bytes() as usize
    }

    /// Returns the maximum payload size for file uploads and chunks.
    pub fn max_api_file_upload_size(&self) -> usize {
        self.values.limits.max_api_file_upload_size.as_bytes() as usize
    }

    /// Returns the maximum payload size for chunks
    pub fn max_api_chunk_upload_size(&self) -> usize {
        self.values.limits.max_api_chunk_upload_size.as_bytes() as usize
    }

    /// Return the Sentry DSN if reporting to Sentry is enabled.
    pub fn sentry_dsn(&self) -> Option<&Dsn> {
        if self.values.sentry.enabled {
            self.values.sentry.dsn.as_ref()
        } else {
            None
        }
    }

    /// Return local project state overrides
    pub fn projects(&self) -> &Projects {
        &self.values.projects
    }
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
    fn format() -> ConfigFormat;
    fn name() -> &'static str;
    fn path(base: &Path) -> PathBuf {
        base.join(format!("{}.{}", Self::name(), Self::format().extension()))
    }

    fn load(base: &Path) -> Result<Self, ConfigError> {
        let path = Self::path(base);
        let f = ctry!(
            fs::File::open(&path),
            ConfigErrorKind::CouldNotOpenFile,
            &path
        );
        Ok(match Self::format() {
            ConfigFormat::Yaml => ctry!(
                serde_yaml::from_reader(io::BufReader::new(f)),
                ConfigErrorKind::BadYaml,
                &path
            ),
            ConfigFormat::Json => ctry!(
                serde_json::from_reader(io::BufReader::new(f)),
                ConfigErrorKind::BadYaml,
                &path
            ),
        })
    }

    fn save(&self, base: &Path) -> Result<(), ConfigError> {
        let path = Self::path(base);
        let mut options = fs::OpenOptions::new();
        options.write(true).truncate(true).create(true);

        // Remove all non-user permissions for the newly created file
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let mut f = ctry!(
            options.open(&path),
            ConfigErrorKind::CouldNotWriteFile,
            &path
        );

        match Self::format() {
            ConfigFormat::Yaml => {
                ctry!(
                    serde_yaml::to_writer(&mut f, self),
                    ConfigErrorKind::BadYaml,
                    &path
                );
            }
            ConfigFormat::Json => {
                ctry!(
                    serde_json::to_writer_pretty(&mut f, self),
                    ConfigErrorKind::BadYaml,
                    &path
                );
            }
        }
        f.write_all(b"\n").ok();
        Ok(())
    }
}
