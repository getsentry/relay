use url_serde;
use url::Url;
use smith_aorta::{SecretKey, PublicKey, AgentId};

lazy_static! {
    static ref SENTRY_INGEST: Url = Url::parse(
        "https://ingest.sentry.io/").unwrap();
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
    #[serde(default)]
    agent: Agent,
}

impl Config {
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
