use serde::{Deserialize, Serialize};

pub(crate) const fn default_max_connections() -> u32 {
    24
}

/// Configuration for connecting a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum RedisConfig {
    /// Connect to a Redis cluster.
    Cluster {
        /// List of `redis://` urls to use in cluster mode.
        ///
        /// This can also be a single node which is configured in cluster mode.
        cluster_nodes: Vec<String>,

        /// Maximum number of connections managed by the pool
        #[serde(default = "default_max_connections")]
        max_connections: u32,
    },

    /// Connect to a single Redis instance.
    ///
    /// Contains the `redis://` url to the node.
    Single(String),

    /// Connect to a single Redis instance
    ///
    /// Allows to provide more configuration options, e.g. `max_connections`
    SingleOpts {
        /// Containes the `redis://` url to the node
        server: String,

        /// Maximum number of connections managed by the pool
        #[serde(default = "default_max_connections")]
        max_connections: u32,
    },
}
