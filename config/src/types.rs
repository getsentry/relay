use std::path::{Path, PathBuf};
use std::io::Write;
use std::fs;
use std::io;
use std::env;
use std::sync::Arc;
use std::net::{IpAddr, SocketAddr};

use log;
use serde_yaml;
use chrono::Duration;
use sentry::Dsn;
use smith_aorta::{generate_key_pair, generate_relay_id, AortaConfig, PublicKey, RelayId,
                  SecretKey, UpstreamDescriptor};

/// Indicates config related errors.
#[derive(Fail, Debug)]
pub enum ConfigError {
    /// Failed to open the file.
    #[fail(display = "could not open config file")]
    CouldNotOpen(#[cause] io::Error),
    /// Failed to save a file.
    #[fail(display = "could not save a config file")]
    CouldNotSave(#[cause] io::Error),
    /// Parsing a YAML error failed.
    #[fail(display = "could not parse yaml file")]
    BadYaml(#[cause] serde_yaml::Error),
}

/// Relay specific configuration values.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Relay {
    secret_key: Option<SecretKey>,
    public_key: Option<PublicKey>,
    id: Option<RelayId>,
    upstream: UpstreamDescriptor<'static>,
    host: IpAddr,
    port: u16,
}

/// Controls the logging system.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Logging {
    level: log::LevelFilter,
}

/// Controls the aorta.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
struct Aorta {
    snapshot_expiry: u32,
    auth_retry_interval: u32,
    heartbeat_interval: u32,
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
            secret_key: None,
            public_key: None,
            id: None,
            upstream: "https://ingest.sentry.io/"
                .parse::<UpstreamDescriptor>()
                .unwrap(),
            host: "127.0.0.1".parse().unwrap(),
            port: 3000,
        }
    }
}

impl Default for Logging {
    fn default() -> Logging {
        Logging {
            level: log::LevelFilter::Info,
        }
    }
}

impl Default for Aorta {
    fn default() -> Aorta {
        Aorta {
            snapshot_expiry: 60,
            auth_retry_interval: 15,
            heartbeat_interval: 30,
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

/// Config struct.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    #[serde(skip, default)]
    changed: bool,
    #[serde(skip, default = "PathBuf::new")]
    filename: PathBuf,
    #[serde(default)]
    relay: Relay,
    #[serde(default)]
    aorta: Aorta,
    #[serde(default)]
    logging: Logging,
    #[serde(default)]
    sentry: Sentry,
}

impl Config {
    /// Loads a config from a given path.
    ///
    /// This can load a config that does not have any credentials yet in
    /// which case some methods will fail (like `secret_key`).  This can
    /// be verified with `is_configured`.  The `open` method handles
    /// this automatically.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let f = fs::File::open(&path).map_err(ConfigError::CouldNotOpen)?;
        let mut rv: Config =
            serde_yaml::from_reader(io::BufReader::new(f)).map_err(ConfigError::BadYaml)?;
        rv.filename = path.as_ref().to_path_buf();
        Ok(rv)
    }

    /// Loads a config from a path or initializes it.
    ///
    /// If the config does not exist or a secret key is not set, then credentials
    /// are regenerated automatically.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let path = match path.as_ref().canonicalize() {
            Ok(pathbuf) => pathbuf,
            Err(_) => env::current_dir()
                .map_err(ConfigError::CouldNotOpen)?
                .join(path.as_ref()),
        };
        let mut config = if fs::metadata(&path).is_ok() {
            Config::from_path(&path)?
        } else {
            Config {
                filename: path,
                changed: false,
                ..Default::default()
            }
        };
        if !config.is_configured() {
            config.regenerate_credentials();
        }
        Ok(config)
    }

    /// Writes back a config to the config file if the config changed.
    pub fn save(&mut self) -> Result<bool, ConfigError> {
        if !self.changed {
            return Ok(false);
        }
        let mut f = fs::File::create(&self.filename).map_err(ConfigError::CouldNotSave)?;
        serde_yaml::to_writer(&mut f, &self).map_err(ConfigError::BadYaml)?;
        f.write_all(b"\n").ok();
        self.changed = false;
        Ok(true)
    }

    /// Returns the filename of the config file.
    pub fn filename(&self) -> &Path {
        &self.filename
    }

    /// Regenerates the relay credentials.
    pub fn regenerate_credentials(&mut self) {
        let (sk, pk) = generate_key_pair();
        self.relay.secret_key = Some(sk);
        self.relay.public_key = Some(pk);
        self.relay.id = Some(generate_relay_id());
        self.changed = true;
    }

    /// Returns `true` if the config changed.
    pub fn changed(&self) -> bool {
        self.changed
    }

    /// Returns `true` if the config is ready to use.
    pub fn is_configured(&self) -> bool {
        self.relay.secret_key.is_some() && self.relay.public_key.is_some()
            && self.relay.id.is_some()
    }

    /// Returns the secret key if set.
    pub fn secret_key(&self) -> &SecretKey {
        self.relay.secret_key.as_ref().unwrap()
    }

    /// Returns the public key if set.
    pub fn public_key(&self) -> &PublicKey {
        self.relay.public_key.as_ref().unwrap()
    }

    /// Returns the relay ID.
    pub fn relay_id(&self) -> &RelayId {
        self.relay.id.as_ref().unwrap()
    }

    /// Returns the upstream target as descriptor.
    pub fn upstream_descriptor(&self) -> &UpstreamDescriptor {
        &self.relay.upstream
    }

    /// Returns the listen address.
    pub fn listen_addr(&self) -> SocketAddr {
        (self.relay.host, self.relay.port).into()
    }

    /// Returns the log level.
    pub fn log_level_filter(&self) -> log::LevelFilter {
        self.logging.level
    }

    /// Returns the aorta snapshot expiry.
    pub fn aorta_snapshot_expiry(&self) -> Duration {
        Duration::seconds(self.aorta.snapshot_expiry as i64)
    }

    /// Returns the aorta auth retry interval.
    pub fn aorta_auth_retry_interval(&self) -> Duration {
        Duration::seconds(self.aorta.auth_retry_interval as i64)
    }

    /// Returns the aorta hearthbeat interval.
    pub fn aorta_heartbeat_interval(&self) -> Duration {
        Duration::seconds(self.aorta.heartbeat_interval as i64)
    }

    /// Return a new aorta config based on this config file.
    pub fn make_aorta_config(&self) -> Arc<AortaConfig> {
        Arc::new(AortaConfig {
            snapshot_expiry: self.aorta_snapshot_expiry(),
            auth_retry_interval: self.aorta_auth_retry_interval(),
            heartbeat_interval: self.aorta_heartbeat_interval(),
            upstream: self.upstream_descriptor().clone().into_owned(),
            relay_id: Some(self.relay_id().clone()),
            secret_key: Some(self.secret_key().clone()),
            public_key: Some(self.public_key().clone()),
        })
    }

    /// Return the Sentry DSN if reporting to Sentry is enabled.
    pub fn sentry_dsn(&self) -> Option<&Dsn> {
        if self.sentry.enabled {
            Some(&self.sentry.dsn)
        } else {
            None
        }
    }
}
