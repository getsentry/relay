use serde::{Deserialize, Serialize};

/// Additional configuration options for a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct PartialRedisConfigOptions {
    /// Maximum number of connections managed by the pool.
    ///
    /// Defaults to 2x `limits.max_thread_count`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
    /// Sets the connection timeout used by the pool, in seconds.
    ///
    /// Calls to `Pool::get` will wait this long for a connection to become available before returning an error.
    pub connection_timeout: u64,
    /// Sets the maximum lifetime of connections in the pool, in seconds.
    pub max_lifetime: u64,
    /// Sets the idle timeout used by the pool, in seconds.
    pub idle_timeout: u64,
    /// Sets the read timeout out on the connection, in seconds.
    pub read_timeout: u64,
    /// Sets the write timeout on the connection, in seconds.
    pub write_timeout: u64,
}

impl Default for PartialRedisConfigOptions {
    fn default() -> Self {
        Self {
            max_connections: None,
            connection_timeout: 5,
            max_lifetime: 300,
            idle_timeout: 60,
            read_timeout: 3,
            write_timeout: 3,
        }
    }
}

/// Configuration for connecting a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
enum RedisConfigFromFile {
    /// Connect to a Redis cluster.
    Cluster {
        /// List of `redis://` urls to use in cluster mode.
        ///
        /// This can also be a single node which is configured in cluster mode.
        cluster_nodes: Vec<String>,

        /// Additional configuration options for the redis client and a connections pool.
        #[serde(flatten)]
        options: PartialRedisConfigOptions,
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
        options: PartialRedisConfigOptions,
    },
}

/// Redis connection parameters.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RedisConnection {
    /// Connect to a Redis Cluster.
    #[serde(rename = "cluster_nodes")]
    Cluster(Vec<String>),
    /// Connect to a single Redis instance.
    #[serde(rename = "server")]
    Single(String),
}

/// Configuration for connecting a redis client.
#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct RedisConfig {
    /// Redis connection info.
    #[serde(flatten)]
    pub connection: RedisConnection,
    /// Additional configuration options for the redis client and a connections pool.
    #[serde(flatten)]
    pub options: PartialRedisConfigOptions,
}

impl RedisConfig {
    /// Creates a new Redis config for a single Redis instance with default settings.
    pub fn single(server: String) -> Self {
        Self {
            connection: RedisConnection::Single(server),
            options: Default::default(),
        }
    }
}

impl From<RedisConfigFromFile> for RedisConfig {
    fn from(value: RedisConfigFromFile) -> Self {
        match value {
            RedisConfigFromFile::Cluster {
                cluster_nodes,
                options,
            } => Self {
                connection: RedisConnection::Cluster(cluster_nodes),
                options,
            },
            RedisConfigFromFile::Single(server) => Self {
                connection: RedisConnection::Single(server),
                options: Default::default(),
            },
            RedisConfigFromFile::SingleWithOpts { server, options } => Self {
                connection: RedisConnection::Single(server),
                options,
            },
        }
    }
}

impl<'de> Deserialize<'de> for RedisConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        RedisConfigFromFile::deserialize(deserializer).map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

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

        assert_eq!(
            config,
            RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    connection_timeout: 5,
                    ..Default::default()
                }
            }
        );
    }

    #[test]
    fn test_redis_single_serialize() {
        let config = RedisConfig {
            connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
            options: PartialRedisConfigOptions {
                connection_timeout: 5,
                ..Default::default()
            },
        };

        assert_json_snapshot!(config, @r###"
        {
          "server": "redis://127.0.0.1:6379",
          "connection_timeout": 5,
          "max_lifetime": 300,
          "idle_timeout": 60,
          "read_timeout": 3,
          "write_timeout": 3
        }
        "###);
    }

    #[test]
    fn test_redis_single_opts_default() {
        let yaml = r#"
server: "redis://127.0.0.1:6379"
"#;

        let config: RedisConfig = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        assert_eq!(
            config,
            RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: Default::default()
            }
        );
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

        assert_eq!(
            config,
            RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: Default::default()
            }
        );
    }

    #[test]
    fn test_redis_cluster_nodes_opts() {
        let yaml = r#"
cluster_nodes:
    - "redis://127.0.0.1:6379"
    - "redis://127.0.0.2:6379"
read_timeout: 10
"#;

        let config: RedisConfig = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        assert_eq!(
            config,
            RedisConfig {
                connection: RedisConnection::Cluster(vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned()
                ]),
                options: PartialRedisConfigOptions {
                    read_timeout: 10,
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn test_redis_cluster_serialize() {
        let config = RedisConfig {
            connection: RedisConnection::Cluster(vec![
                "redis://127.0.0.1:6379".to_owned(),
                "redis://127.0.0.2:6379".to_owned(),
            ]),
            options: PartialRedisConfigOptions {
                read_timeout: 33,
                ..Default::default()
            },
        };

        assert_json_snapshot!(config, @r###"
        {
          "cluster_nodes": [
            "redis://127.0.0.1:6379",
            "redis://127.0.0.2:6379"
          ],
          "connection_timeout": 5,
          "max_lifetime": 300,
          "idle_timeout": 60,
          "read_timeout": 33,
          "write_timeout": 3
        }
        "###);
    }
}
