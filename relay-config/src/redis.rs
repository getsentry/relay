use relay_redis::RedisConfigOptions;
use serde::{Deserialize, Serialize};

/// For small setups, `2 x limits.max_thread_count` does not leave enough headroom.
/// In this case, we fall back to the old default.
pub(crate) const DEFAULT_MIN_MAX_CONNECTIONS: u32 = 24;

/// Additional configuration options for a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct PartialRedisConfigOptions {
    /// Maximum number of connections managed by the pool.
    ///
    /// Defaults to 2x `limits.max_thread_count` or a minimum of 24.
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
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(from = "RedisConfigFromFile")]
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

/// Configurations for the various Redis pools used by Relay.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum RedisConfigs {
    /// All pools should be configured the same way.
    Unified(RedisConfig),
    /// Individual configurations for each pool.
    Individual {
        /// Configuration for the `project_configs` pool.
        project_configs: Box<RedisConfig>,
        /// Configuration for the `cardinality` pool.
        cardinality: Box<RedisConfig>,
        /// Configuration for the `quotas` pool.
        quotas: Box<RedisConfig>,
        /// Configuration for the `misc` pool.
        misc: Box<RedisConfig>,
    },
}

/// Helper struct bundling connections and options for the various Redis pools.
#[derive(Clone, Debug)]
pub struct RedisPoolConfigs<'a> {
    /// Configuration for the `project_configs` pool.
    pub project_configs: (&'a RedisConnection, RedisConfigOptions),
    /// Configuration for the `cardinality` pool.
    pub cardinality: (&'a RedisConnection, RedisConfigOptions),
    /// Configuration for the `quotas` pool.
    pub quotas: (&'a RedisConnection, RedisConfigOptions),
    /// Configuration for the `misc` pool.
    pub misc: (&'a RedisConnection, RedisConfigOptions),
}

pub(super) fn create_redis_pool(
    config: &RedisConfig,
    default_connections: u32,
) -> (&RedisConnection, RedisConfigOptions) {
    let options = RedisConfigOptions {
        max_connections: config
            .options
            .max_connections
            .unwrap_or(default_connections),
        connection_timeout: config.options.connection_timeout,
        max_lifetime: config.options.max_lifetime,
        idle_timeout: config.options.idle_timeout,
        read_timeout: config.options.read_timeout,
        write_timeout: config.options.write_timeout,
    };

    (&config.connection, options)
}

pub(super) fn create_redis_pools(configs: &RedisConfigs, cpu_concurrency: u32) -> RedisPoolConfigs {
    match configs {
        RedisConfigs::Unified(cfg) => {
            let default_connections =
                (cpu_concurrency * 2).min(crate::redis::DEFAULT_MIN_MAX_CONNECTIONS);
            let pool = create_redis_pool(cfg, default_connections);
            RedisPoolConfigs {
                project_configs: pool.clone(),
                cardinality: pool.clone(),
                quotas: pool.clone(),
                misc: pool,
            }
        }
        RedisConfigs::Individual {
            project_configs,
            cardinality,
            quotas,
            misc,
        } => {
            let project_configs = create_redis_pool(
                project_configs,
                (cpu_concurrency * 2).min(crate::redis::DEFAULT_MIN_MAX_CONNECTIONS),
            );
            let cardinality = create_redis_pool(cardinality, cpu_concurrency);
            let quotas = create_redis_pool(quotas, cpu_concurrency);
            let misc = create_redis_pool(misc, cpu_concurrency);
            RedisPoolConfigs {
                project_configs,
                cardinality,
                quotas,
                misc,
            }
        }
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
    fn test_redis_single_opts_unified() {
        let yaml = r#"
server: "redis://127.0.0.1:6379"
max_connections: 42
connection_timeout: 5
"#;

        let config: RedisConfigs = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        assert_eq!(
            config,
            RedisConfigs::Unified(RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    connection_timeout: 5,
                    ..Default::default()
                }
            })
        );
    }

    #[test]
    fn test_redis_individual() {
        let yaml = r#"
project_configs:
    server: "redis://127.0.0.1:6379"
    max_connections: 42
    connection_timeout: 5
cardinality:
    server: "redis://127.0.0.1:6379"
quotas: 
    cluster_nodes:
        - "redis://127.0.0.1:6379"
        - "redis://127.0.0.2:6379"
    max_connections: 17
    connection_timeout: 5
misc:
    cluster_nodes:
        - "redis://127.0.0.1:6379"
        - "redis://127.0.0.2:6379"
"#;

        let configs: RedisConfigs = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis configs: single with options");

        let expected = RedisConfigs::Individual {
            project_configs: Box::new(RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            cardinality: Box::new(RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: Default::default(),
            }),
            quotas: Box::new(RedisConfig {
                connection: RedisConnection::Cluster(vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ]),
                options: PartialRedisConfigOptions {
                    max_connections: Some(17),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            misc: Box::new(RedisConfig {
                connection: RedisConnection::Cluster(vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ]),
                options: Default::default(),
            }),
        };

        assert_eq!(configs, expected);
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
    fn test_redis_single_serialize_unified() {
        let configs = RedisConfigs::Unified(RedisConfig {
            connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
            options: PartialRedisConfigOptions {
                connection_timeout: 5,
                ..Default::default()
            },
        });

        assert_json_snapshot!(configs, @r###"
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
    fn test_redis_cluster_nodes_opts_unified() {
        let yaml = r#"
cluster_nodes:
    - "redis://127.0.0.1:6379"
    - "redis://127.0.0.2:6379"
read_timeout: 10
"#;

        let config: RedisConfigs = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        assert_eq!(
            config,
            RedisConfigs::Unified(RedisConfig {
                connection: RedisConnection::Cluster(vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned()
                ]),
                options: PartialRedisConfigOptions {
                    read_timeout: 10,
                    ..Default::default()
                },
            })
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

    #[test]
    fn test_redis_cluster_serialize_unified() {
        let configs = RedisConfigs::Unified(RedisConfig {
            connection: RedisConnection::Cluster(vec![
                "redis://127.0.0.1:6379".to_owned(),
                "redis://127.0.0.2:6379".to_owned(),
            ]),
            options: PartialRedisConfigOptions {
                read_timeout: 33,
                ..Default::default()
            },
        });

        assert_json_snapshot!(configs, @r###"
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

    #[test]
    fn test_redis_serialize_individual() {
        let configs = RedisConfigs::Individual {
            project_configs: Box::new(RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            cardinality: Box::new(RedisConfig {
                connection: RedisConnection::Single("redis://127.0.0.1:6379".to_owned()),
                options: Default::default(),
            }),
            quotas: Box::new(RedisConfig {
                connection: RedisConnection::Cluster(vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ]),
                options: PartialRedisConfigOptions {
                    max_connections: Some(17),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            misc: Box::new(RedisConfig {
                connection: RedisConnection::Cluster(vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ]),
                options: Default::default(),
            }),
        };

        assert_json_snapshot!(configs, @r###"
       {
         "project_configs": {
           "server": "redis://127.0.0.1:6379",
           "max_connections": 42,
           "connection_timeout": 5,
           "max_lifetime": 300,
           "idle_timeout": 60,
           "read_timeout": 3,
           "write_timeout": 3
         },
         "cardinality": {
           "server": "redis://127.0.0.1:6379",
           "connection_timeout": 5,
           "max_lifetime": 300,
           "idle_timeout": 60,
           "read_timeout": 3,
           "write_timeout": 3
         },
         "quotas": {
           "cluster_nodes": [
             "redis://127.0.0.1:6379",
             "redis://127.0.0.2:6379"
           ],
           "max_connections": 17,
           "connection_timeout": 5,
           "max_lifetime": 300,
           "idle_timeout": 60,
           "read_timeout": 3,
           "write_timeout": 3
         },
         "misc": {
           "cluster_nodes": [
             "redis://127.0.0.1:6379",
             "redis://127.0.0.2:6379"
           ],
           "connection_timeout": 5,
           "max_lifetime": 300,
           "idle_timeout": 60,
           "read_timeout": 3,
           "write_timeout": 3
         }
       }
        "###);
    }
}
