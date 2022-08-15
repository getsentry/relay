use serde::{Deserialize, Serialize};

const fn default_max_connections() -> u32 {
    24
}

/// Additional configuration options for a redis client
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RedisConfigOptions {
    /// Maximum number of connections managed by the pool
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// If true, the health of a connection will be verified before it's checked out of the pool
    #[serde(skip, default)]
    pub test_on_check_out: bool,
}

impl Default for RedisConfigOptions {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            test_on_check_out: bool::default(),
        }
    }
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

        /// Additional configuration options for the redis client and a connections pool
        #[serde(flatten)]
        options: RedisConfigOptions,
    },

    /// Connect to a single Redis instance.
    ///
    /// Contains the `redis://` url to the node.
    Single(String),

    /// Connect to a single Redis instance
    ///
    /// Allows to provide more configuration options, e.g. `max_connections`
    SingleWithOpts {
        /// Containes the `redis://` url to the node
        server: String,

        /// Additional configuration options for the redis client and a connections pool
        #[serde(flatten)]
        options: RedisConfigOptions,
    },
}
