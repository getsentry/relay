use serde::{Deserialize, Serialize};

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
    },

    /// Connect to a single Redis instance.
    ///
    /// Contains the `redis://` url to the node.
    Single(String),
}
