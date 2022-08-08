use serde::{Deserialize, Serialize};

fn default_connections() -> u32 {
    8
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

        /// The maximum number of concurrent connections to the cluster.
        ///
        /// Defaults to 8.
        #[serde(default = "default_connections")]
        max_connections: u32,
    },

    /// Connect to a single Redis instance.
    ///
    /// Contains the `redis://` url to the node.
    Single(String),
}
