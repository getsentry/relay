use std::fs;
use std::io;
use std::io::Write;
use std::fmt;
use std::env;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Duration;
use log;
use sentry::Dsn;
use serde_json;
use serde_yaml;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use failure::{Backtrace, Context, Fail};
use semaphore_aorta::{generate_key_pair, generate_relay_id, AortaConfig, PublicKey, RelayId,
                      SecretKey, UpstreamDescriptor};

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
            Err(err) => return Err(ConfigError::new($path, err.context($kind)))
        }
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
#[derive(Serialize, Deserialize, Debug)]
struct Credentials {
    /// The secret key of the relay
    secret_key: SecretKey,
    /// The public key of the relay
    public_key: PublicKey,
    /// The globally unique ID of the relay.
    id: RelayId,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_port: Option<u16>,
    /// The path to the private key to use for TLS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_private_key: Option<PathBuf>,
    /// The path to the certificate chain to use for TLS
    #[serde(skip_serializing_if = "Option::is_none")]
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

fn load_config_from_path<D: DeserializeOwned>(path: &Path) -> Result<D, ConfigError> {
    let f = ctry!(
        fs::File::open(path),
        ConfigErrorKind::CouldNotOpenFile,
        path
    );
    Ok(ctry!(
        serde_yaml::from_reader(io::BufReader::new(f)),
        ConfigErrorKind::BadYaml,
        path
    ))
}

fn save_config_to_path<S: Serialize>(path: &Path, s: &S) -> Result<(), ConfigError> {
    let mut f = ctry!(
        fs::File::create(path),
        ConfigErrorKind::CouldNotWriteFile,
        path
    );
    ctry!(
        serde_yaml::to_writer(&mut f, s),
        ConfigErrorKind::BadYaml,
        path
    );
    f.write_all(b"\n").ok();
    Ok(())
}

fn get_config_path(path: &Path, file: &str) -> PathBuf {
    path.join(format!("{}.yml", file))
}

impl Config {
    /// Loads a config from a given config folder.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let path = env::current_dir()
            .map(|x| x.join(path.as_ref()))
            .unwrap_or_else(|_| path.as_ref().to_path_buf());
        let values = load_config_from_path(&get_config_path(&path, "config"))?;
        let credentials_path = get_config_path(&path, "credentials");
        let credentials = if fs::metadata(&credentials_path).is_ok() {
            Some(load_config_from_path(&credentials_path)?)
        } else {
            None
        };
        Ok(Config {
            path,
            credentials,
            values,
        })
    }

    /// Checks if the config is already initialized.
    pub fn config_exists<P: AsRef<Path>>(path: P) -> bool {
        fs::metadata(get_config_path(path.as_ref(), "config")).is_ok()
    }

    /// Returns the filename of the config file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Dumps out a JSON string of the values.
    pub fn to_json_string(&self) -> Result<String, ConfigError> {
        Ok(ctry!(
            serde_json::to_string_pretty(&self.values),
            ConfigErrorKind::InvalidValue,
            &self.path
        ))
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
        save_config_to_path(&get_config_path(&self.path, "credentials"), &creds)?;
        self.credentials = Some(creds);
        Ok(())
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

    /// Return a new aorta config based on this config file.
    pub fn make_aorta_config(&self) -> Arc<AortaConfig> {
        Arc::new(AortaConfig {
            snapshot_expiry: self.aorta_snapshot_expiry(),
            auth_retry_interval: self.aorta_auth_retry_interval(),
            heartbeat_interval: self.aorta_heartbeat_interval(),
            changeset_buffer_interval: self.aorta_changeset_buffer_interval(),
            pending_events_timeout: self.aorta_pending_events_timeout(),
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
        save_config_to_path(&get_config_path(p.as_ref(), "config"), self)
    }
}
