use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Duration;
use failure::{Backtrace, Context, Fail};
use log;
use semaphore_aorta::{generate_key_pair, generate_relay_id, AortaConfig, PublicKey, RelayId,
                      SecretKey, UpstreamDescriptor};
use sentry::Dsn;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;
use serde_yaml;

/// Indicates config related errors.
#[derive(Debug)]
pub struct ConfigError {
    filename: PathBuf,
    inner: Context<ConfigErrorKind>,
}

macro_rules! ctry {
    ($expr:expr, $kind:expr, $path:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ConfigError::new($path, err.context($kind))),
        }
    };
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

impl ConfigError {
    fn new<P: AsRef<Path>>(p: P, inner: Context<ConfigErrorKind>) -> ConfigError {
        ConfigError {
            filename: p.as_ref().to_path_buf(),
            inner: inner,
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
    /// The path to the private key to use for TLS
    pub tls_private_key: Option<PathBuf>,
    /// The path to the certificate chain to use for TLS
    pub tls_cert: Option<PathBuf>,
}

/// Minimal version of a config for dumping out.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MinimalConfig {
    /// The relay part of the config.
    pub relay: Relay,
}

/// Controls the logging system.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Logging {
    /// The log level for the relay.
    level: log::LevelFilter,
    /// If set to true this emits log messages for failed event payloads.
    log_failed_payloads: bool,
    /// When set to true, backtraces are forced on.
    enable_backtraces: bool,
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

/// Controls the aorta.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Aorta {
    /// The number of seconds after which a relay is considered outdated.
    snapshot_expiry: u32,
    /// The number of seconds between auth attempts.
    auth_retry_interval: u32,
    /// The number of seconds after which a heartbeat is forced.
    heartbeat_interval: u32,
    /// The number of seconds that the relay should buffer changesets for.
    changeset_buffer_interval: u32,
    /// The number of seconds for which an event shoudl be buffered until the
    /// initial config arrives.
    pending_events_timeout: u32,
    /// The maximum payload size for events.
    max_event_payload_size: u32,
}

/// Controls interal reporting to Sentry.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Sentry {
    dsn: Dsn,
    enabled: bool,
}

impl Default for Relay {
    fn default() -> Relay {
        Relay {
            upstream: "https://ingest.sentry.io/"
                .parse::<UpstreamDescriptor>()
                .unwrap(),
            host: "127.0.0.1".parse().unwrap(),
            port: 3000,
            tls_port: None,
            tls_private_key: None,
            tls_cert: None,
        }
    }
}

impl Default for Logging {
    fn default() -> Logging {
        Logging {
            level: log::LevelFilter::Info,
            log_failed_payloads: false,
            enable_backtraces: true,
        }
    }
}

impl Default for Metrics {
    fn default() -> Metrics {
        Metrics {
            statsd: None,
            prefix: "sentry.relay".into(),
        }
    }
}

impl Default for Aorta {
    fn default() -> Aorta {
        Aorta {
            snapshot_expiry: 60,
            auth_retry_interval: 15,
            heartbeat_interval: 60,
            changeset_buffer_interval: 2,
            pending_events_timeout: 60,
            max_event_payload_size: 524_288,
        }
    }
}

impl Default for Sentry {
    fn default() -> Sentry {
        Sentry {
            dsn: "https://1bb6015c9e064924890685d6311e0344@sentry.io/1195971"
                .parse()
                .unwrap(),
            enabled: true,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ConfigValues {
    #[serde(default)]
    relay: Relay,
    #[serde(default)]
    aorta: Aorta,
    #[serde(default)]
    logging: Logging,
    #[serde(default)]
    metrics: Metrics,
    #[serde(default)]
    sentry: Sentry,
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

impl ConfigObject for Credentials {
    fn format() -> ConfigFormat {
        ConfigFormat::Json
    }
    fn name() -> &'static str {
        "credentials"
    }
}

impl ConfigObject for ConfigValues {
    fn format() -> ConfigFormat {
        ConfigFormat::Yaml
    }
    fn name() -> &'static str {
        "config"
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
        if self.values.relay.tls_private_key.is_some() && self.values.relay.tls_cert.is_some() {
            let port = self.values.relay.tls_port.unwrap_or(3443);
            Some((self.values.relay.host, port).into())
        } else {
            None
        }
    }

    /// Returns the path to the private key
    pub fn tls_private_key_path(&self) -> Option<&Path> {
        self.values
            .relay
            .tls_private_key
            .as_ref()
            .map(|x| x.as_path())
    }

    /// Returns the path to the cert
    pub fn tls_certificate_path(&self) -> Option<&Path> {
        self.values.relay.tls_cert.as_ref().map(|x| x.as_path())
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

    /// Returns the aorta snapshot expiry.
    pub fn aorta_snapshot_expiry(&self) -> Duration {
        Duration::seconds(self.values.aorta.snapshot_expiry as i64)
    }

    /// Returns the aorta auth retry interval.
    pub fn aorta_auth_retry_interval(&self) -> Duration {
        Duration::seconds(self.values.aorta.auth_retry_interval as i64)
    }

    /// Returns the aorta hearthbeat interval.
    pub fn aorta_heartbeat_interval(&self) -> Duration {
        Duration::seconds(self.values.aorta.heartbeat_interval as i64)
    }

    /// Returns the aorta changeset buffer interval.
    pub fn aorta_changeset_buffer_interval(&self) -> Duration {
        Duration::seconds(self.values.aorta.changeset_buffer_interval as i64)
    }

    /// Returns the timeout for pending events.
    pub fn aorta_pending_events_timeout(&self) -> Duration {
        Duration::seconds(self.values.aorta.pending_events_timeout as i64)
    }

    /// Returns the maximum size of an event payload in bytes.
    pub fn aorta_max_event_payload_size(&self) -> usize {
        self.values.aorta.max_event_payload_size as usize
    }

    /// Return a new aorta config based on this config file.
    pub fn make_aorta_config(&self) -> Arc<AortaConfig> {
        Arc::new(AortaConfig {
            snapshot_expiry: self.aorta_snapshot_expiry(),
            auth_retry_interval: self.aorta_auth_retry_interval(),
            heartbeat_interval: self.aorta_heartbeat_interval(),
            changeset_buffer_interval: self.aorta_changeset_buffer_interval(),
            pending_events_timeout: self.aorta_pending_events_timeout(),
            max_event_payload_size: self.aorta_max_event_payload_size(),
            upstream: self.upstream_descriptor().clone().into_owned(),
            relay_id: Some(self.relay_id().clone()),
            secret_key: Some(self.secret_key().clone()),
            public_key: Some(self.public_key().clone()),
        })
    }

    /// Return the Sentry DSN if reporting to Sentry is enabled.
    pub fn sentry_dsn(&self) -> Option<&Dsn> {
        if self.values.sentry.enabled {
            Some(&self.values.sentry.dsn)
        } else {
            None
        }
    }
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
