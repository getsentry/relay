use std::path::{Path, PathBuf};
use std::fs;
use std::io;

use url_serde;
use serde_yaml;
use url::Url;
use smith_aorta::{SecretKey, PublicKey, AgentId, generate_key_pair,
                  generate_agent_id};

lazy_static! {
    static ref SENTRY_INGEST: Url = Url::parse(
        "https://ingest.sentry.io/").unwrap();
}

#[derive(Fail, Debug)]
pub enum ConfigError {
    /// Failed to open the file.
    #[fail(display="could not open config file")]
    CouldNotOpen(#[cause] io::Error),
    /// Failed to save a file.
    #[fail(display="could not save a config file")]
    CouldNotSave(#[cause] io::Error),
    /// Parsing a YAML error failed.
    #[fail(display="could not parse yaml file")]
    BadYaml(#[cause] serde_yaml::Error),
}

/// Agent specific configuration values.
#[derive(Serialize, Deserialize, Debug, Default)]
struct Agent {
    secret_key: Option<SecretKey>,
    public_key: Option<PublicKey>,
    id: Option<AgentId>,
    #[serde(with="url_serde")]
    upstream: Option<Url>,
}

/// Config struct.
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    #[serde(skip, default="PathBuf::new")]
    filename: PathBuf,
    #[serde(default)]
    agent: Agent,
}

impl Config {
    /// Loads a config from a given path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let f = fs::File::open(path.as_ref()).map_err(ConfigError::CouldNotOpen)?;
        let mut rv: Config = serde_yaml::from_reader(io::BufReader::new(f))
            .map_err(ConfigError::BadYaml)?;
        rv.filename = path.as_ref().to_path_buf();
        Ok(rv)
    }

    /// Writes back a config to the config file.
    pub fn save(&self) -> Result<(), ConfigError> {
        let mut f = fs::File::open(&self.filename).map_err(ConfigError::CouldNotSave)?;
        serde_yaml::to_writer(&mut f, &self).map_err(ConfigError::BadYaml)?;
        Ok(())
    }

    /// Regenerates the agent credentials.
    pub fn regenerate_credentials(&mut self) {
        let (sk, pk) = generate_key_pair();
        self.agent.secret_key = Some(sk);
        self.agent.public_key = Some(pk);
        self.agent.id = Some(generate_agent_id());
    }

    /// Returns the secret key if set.
    pub fn secret_key(&self) -> Option<&SecretKey> {
        self.agent.secret_key.as_ref()
    }

    /// Returns the public key if set.
    pub fn public_key(&self) -> Option<&PublicKey> {
        self.agent.public_key.as_ref()
    }

    /// Returns the agent ID.
    pub fn agent_id(&self) -> Option<&AgentId> {
        self.agent.id.as_ref()
    }

    /// Returns the upstream target.
    pub fn upstream_target(&self) -> &Url {
        self.agent.upstream.as_ref().unwrap_or(&SENTRY_INGEST)
    }
}
