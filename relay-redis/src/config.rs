use serde::{Deserialize, Serialize};

const fn default_max_connections() -> u32 {
    24
}

const fn default_connection_timeout() -> u64 {
    5
}

const fn default_max_lifetime() -> u64 {
    300
}

const fn default_idle_timeout() -> u64 {
    60
}

const fn default_read_timeout() -> u64 {
    3
}

const fn default_write_timeout() -> u64 {
    3
}

/// Additional configuration options for a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct RedisConfigOptions {
    /// Maximum number of connections managed by the pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Sets the connection timeout used by the pool, in seconds.
    ///
    /// Calls to `Pool::get` will wait this long for a connection to become available before returning an error.
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u64,
    /// Sets the maximum lifetime of connections in the pool, in seconds.
    #[serde(default = "default_max_lifetime")]
    pub max_lifetime: u64,
    /// Sets the idle timeout used by the pool, in seconds.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: u64,
    /// Sets the read timeout out on the connection, in seconds.
    #[serde(default = "default_read_timeout")]
    pub read_timeout: u64,
    /// Sets the write timeout on the connection, in seconds.
    #[serde(default = "default_write_timeout")]
    pub write_timeout: u64,
}

impl Default for RedisConfigOptions {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            connection_timeout: default_connection_timeout(),
            max_lifetime: default_max_lifetime(),
            idle_timeout: default_idle_timeout(),
            read_timeout: default_read_timeout(),
            write_timeout: default_write_timeout(),
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

        /// Additional configuration options for the redis client and a connections pool.
        #[serde(flatten)]
        options: RedisConfigOptions,
    },

    /// Connect to a single Redis instance.
    ///
    /// Contains the `redis://` url to the node.
    Single(String),

    /// Connect to a single Redis instance.
    ///
    /// Allows to provide more configuration options, e.g. `max_connections`.
    SingleWithOpts {
        /// Containes the `redis://` url to the node.
        server: String,

        /// Additional configuration options for the redis client and a connections pool.
        #[serde(flatten)]
        options: RedisConfigOptions,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_single_opts() {
        let yaml = r#"
server: "redis://127.0.0.1:6379"
max_connections: 42
connection_timeout: 5
"#;

        let config: RedisConfig = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        match config {
            RedisConfig::SingleWithOpts { server, options } => {
                assert_eq!(options.max_connections, 42);
                assert_eq!(options.connection_timeout, 5);
                assert_eq!(server, "redis://127.0.0.1:6379");
            }
            e => panic!("Expected RedisConfig::SingleWithOpts but got {e:?}"),
        }
    }

    #[test]
    fn test_redis_single_opts_default() {
        let yaml = r#"
server: "redis://127.0.0.1:6379"
"#;

        let config: RedisConfig = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        match config {
            RedisConfig::SingleWithOpts { options, .. } => {
                // check if all the defaults are correctly set
                assert_eq!(options.max_connections, 24);
            }
            e => panic!("Expected RedisConfig::SingleWithOpts but got {e:?}"),
        }
    }

    // To make sure that we have backwards compatibility and still support the redis configuration
    // when the single `redis://...` address is provided.
    #[test]
    fn test_redis_single() {
        let yaml = r#"
"redis://127.0.0.1:6379"
"#;

        let config: RedisConfig = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        match config {
            RedisConfig::Single(server) => {
                assert_eq!(server, "redis://127.0.0.1:6379");
            }
            e => panic!("Expected RedisConfig::Single but got {e:?}"),
        }
    }
}

#[test]
fn test_redis_cluster_nodes_opts() {
    let yaml = r#"
cluster_nodes:
    - "redis://127.0.0.1:6379"
    - "redis://127.0.0.2:6379"
read_timeout: 10
"#;

    let config: RedisConfig =
        serde_yaml::from_str(yaml).expect("Parsed processing redis config: single with options");

    match config {
        RedisConfig::Cluster {
            cluster_nodes,
            options,
        } => {
            assert_eq!(options.max_connections, 24);
            assert_eq!(options.connection_timeout, 5);
            assert_eq!(options.read_timeout, 10);
            assert_eq!(options.write_timeout, 3);
            assert!(cluster_nodes.contains(&String::from("redis://127.0.0.2:6379")));
        }
        e => panic!("Expected RedisConfig::SingleWithOpts but got {e:?}"),
    }
}
