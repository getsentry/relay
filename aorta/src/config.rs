use chrono::Duration;

use auth::{AgentId, PublicKey, SecretKey};
use upstream::UpstreamDescriptor;

/// Holds common config values that affect the aorta behavior.
///
/// This config is typically created by something and then passed down
/// through the aorta functionality to affect how they behave.  This is
/// in turn also used by the trove crate to manage the individual aortas.
#[derive(Debug)]
pub struct AortaConfig {
    /// How long it takes until a snapshot is considered expired.
    pub snapshot_expiry: Duration,
    /// The upstream descriptor for this aorta
    pub upstream: UpstreamDescriptor<'static>,
    /// The agent ID.
    pub agent_id: Option<AgentId>,
    /// The private key for authentication.
    pub secret_key: Option<SecretKey>,
    /// The public key for authentication.
    pub public_key: Option<PublicKey>,
}

impl Default for AortaConfig {
    fn default() -> AortaConfig {
        AortaConfig {
            snapshot_expiry: Duration::seconds(60),
            upstream: Default::default(),
            agent_id: None,
            secret_key: None,
            public_key: None,
        }
    }
}
