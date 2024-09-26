use relay_redis::RedisConfigOptions;
use serde::{Deserialize, Serialize};

/// For small setups, `2 x limits.max_thread_count` does not leave enough headroom.
/// In this case, we fall back to the old default.
pub(crate) const DEFAULT_MIN_MAX_CONNECTIONS: u32 = 24;

/// By default, the `min_idle` count of the Redis pool is set to the calculated
/// amount of max connections divided by this value and rounded up.
///
/// To express this value as a percentage of max connections,
/// use this formula: `100 / DEFAULT_MIN_IDLE_RATIO`.
pub(crate) const DEFAULT_MIN_IDLE_RATIO: u32 = 5;

/// Additional configuration options for a redis client.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct PartialRedisConfigOptions {
    /// Maximum number of connections managed by the pool.
    ///
    /// Defaults to 2x `limits.max_thread_count` or a minimum of 24.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
    /// Minimum amount of idle connections kept alive in the pool.
    ///
    /// If not set it will default to 20% of [`Self::max_connections`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_idle: Option<u32>,
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
            min_idle: None,
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

    /// Connect to a set of Redis instances for multiple writes.
    MultiWrite(Vec<RedisConfigFromFile>),

    /// Connect to a single Redis instance.
    ///
    /// Contains the `redis://` url to the node.
    Single(String),

    /// Connect to a single Redis instance.
    ///
    /// Allows to provide more configuration options, e.g. `max_connections`.
    SingleWithOpts {
        /// Contains the `redis://` url to the node.
        server: String,

        /// Additional configuration options for the redis client and a connections pool.
        #[serde(flatten)]
        options: PartialRedisConfigOptions,
    },
}

/// Redis configuration.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum RedisConfig {
    /// Connect to a Redis Cluster.
    Cluster {
        /// Redis nodes urls of the cluster.
        cluster_nodes: Vec<String>,
        /// Options of the Redis config.
        #[serde(flatten)]
        options: PartialRedisConfigOptions,
    },
    /// Connect to multiple Redis instances for multiple writes.
    MultiWrite {
        /// Configurations for the Redis instances.
        configs: Vec<RedisConfig>,
    },
    /// Connect to a single Redis instance.
    Single {
        /// Redis node url.
        server: String,
        /// Options of the Redis config.
        #[serde(flatten)]
        options: PartialRedisConfigOptions,
    },
}

impl RedisConfig {
    /// Creates a new Redis config for a single Redis instance with default settings.
    pub fn single(server: String) -> Self {
        RedisConfig::Single {
            server,
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
            } => Self::Cluster {
                cluster_nodes,
                options,
            },
            RedisConfigFromFile::MultiWrite(configs) => Self::MultiWrite {
                configs: configs.into_iter().map(|c| c.into()).collect(),
            },
            RedisConfigFromFile::Single(server) => Self::Single {
                server,
                options: Default::default(),
            },
            RedisConfigFromFile::SingleWithOpts { server, options } => {
                Self::Single { server, options }
            }
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

/// Reference to the [`RedisConfig`] with the final [`RedisConfigOptions`].
#[derive(Clone, Debug)]
pub enum RedisConfigRef<'a> {
    /// Connect to a Redis Cluster.
    Cluster {
        /// Reference to the Redis nodes urls of the cluster.
        cluster_nodes: &'a Vec<String>,
        /// Options of the Redis config.
        options: RedisConfigOptions,
    },
    /// Connect to multiple Redis instances for multiple writes.
    MultiWrite {
        /// Configurations for the Redis instances.
        configs: Vec<RedisConfigRef<'a>>,
    },
    /// Connect to a single Redis instance.
    Single {
        /// Reference to the Redis node url.
        server: &'a String,
        /// Options of the Redis config.
        options: RedisConfigOptions,
    },
}

/// Helper struct bundling connections and options for the various Redis pools.
#[derive(Clone, Debug)]
pub enum RedisPoolConfigs<'a> {
    /// Use one pool for everything.
    Unified(RedisConfigRef<'a>),
    /// Use an individual pool for each use case.
    Individual {
        /// Configuration for the `project_configs` pool.
        project_configs: RedisConfigRef<'a>,
        /// Configuration for the `cardinality` pool.
        cardinality: RedisConfigRef<'a>,
        /// Configuration for the `quotas` pool.
        quotas: RedisConfigRef<'a>,
        /// Configuration for the `misc` pool.
        misc: RedisConfigRef<'a>,
    },
}

fn build_redis_config_options(
    options: &PartialRedisConfigOptions,
    default_connections: u32,
) -> RedisConfigOptions {
    let max_connections = options.max_connections.unwrap_or(default_connections);
    let min_idle = options
        .min_idle
        .unwrap_or_else(|| max_connections.div_ceil(crate::redis::DEFAULT_MIN_IDLE_RATIO));

    RedisConfigOptions {
        max_connections,
        min_idle: Some(min_idle),
        connection_timeout: options.connection_timeout,
        max_lifetime: options.max_lifetime,
        idle_timeout: options.idle_timeout,
        read_timeout: options.read_timeout,
        write_timeout: options.write_timeout,
    }
}

pub(super) fn create_redis_pool(
    config: &RedisConfig,
    default_connections: u32,
) -> RedisConfigRef<'_> {
    match config {
        RedisConfig::Cluster {
            cluster_nodes,
            options,
            ..
        } => RedisConfigRef::Cluster {
            cluster_nodes,
            options: build_redis_config_options(options, default_connections),
        },
        RedisConfig::MultiWrite { configs } => RedisConfigRef::MultiWrite {
            configs: configs
                .iter()
                .map(|c| create_redis_pool(c, default_connections))
                .collect(),
        },
        RedisConfig::Single {
            server, options, ..
        } => RedisConfigRef::Single {
            server,
            options: build_redis_config_options(options, default_connections),
        },
    }
}

pub(super) fn create_redis_pools(configs: &RedisConfigs, cpu_concurrency: u32) -> RedisPoolConfigs {
    // Default `max_connections` for the `project_configs` pool.
    // In a unified config, this is used for all pools.
    let project_configs_default_connections = std::cmp::max(
        cpu_concurrency * 2,
        crate::redis::DEFAULT_MIN_MAX_CONNECTIONS,
    );

    match configs {
        RedisConfigs::Unified(cfg) => {
            let pool = create_redis_pool(cfg, project_configs_default_connections);
            RedisPoolConfigs::Unified(pool)
        }
        RedisConfigs::Individual {
            project_configs,
            cardinality,
            quotas,
            misc,
        } => {
            let project_configs =
                create_redis_pool(project_configs, project_configs_default_connections);
            let cardinality = create_redis_pool(cardinality, cpu_concurrency);
            let quotas = create_redis_pool(quotas, cpu_concurrency);
            let misc = create_redis_pool(misc, cpu_concurrency);
            RedisPoolConfigs::Individual {
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
            RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
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
            RedisConfigs::Unified(RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
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
    configs:
        - cluster_nodes:
            - "redis://127.0.0.1:6379"
            - "redis://127.0.0.2:6379"
          max_connections: 42
          connection_timeout: 5
        - server: "redis://127.0.0.1:6379"
          max_connections: 84
          connection_timeout: 10
"#;

        let configs: RedisConfigs = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis configs: single with options");

        let expected = RedisConfigs::Individual {
            project_configs: Box::new(RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            cardinality: Box::new(RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: Default::default(),
            }),
            quotas: Box::new(RedisConfig::Cluster {
                cluster_nodes: vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ],
                options: PartialRedisConfigOptions {
                    max_connections: Some(17),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            misc: Box::new(RedisConfig::MultiWrite {
                configs: vec![
                    RedisConfig::Cluster {
                        cluster_nodes: vec![
                            "redis://127.0.0.1:6379".to_owned(),
                            "redis://127.0.0.2:6379".to_owned(),
                        ],
                        options: PartialRedisConfigOptions {
                            max_connections: Some(42),
                            connection_timeout: 5,
                            ..Default::default()
                        },
                    },
                    RedisConfig::Single {
                        server: "redis://127.0.0.1:6379".to_owned(),
                        options: PartialRedisConfigOptions {
                            max_connections: Some(84),
                            connection_timeout: 10,
                            ..Default::default()
                        },
                    },
                ],
            }),
        };

        assert_eq!(configs, expected);
    }

    #[test]
    fn test_redis_single_serialize() {
        let config = RedisConfig::Single {
            server: "redis://127.0.0.1:6379".to_owned(),
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
        let configs = RedisConfigs::Unified(RedisConfig::Single {
            server: "redis://127.0.0.1:6379".to_owned(),
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
            RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
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
            RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
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
            RedisConfig::Cluster {
                cluster_nodes: vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned()
                ],
                options: PartialRedisConfigOptions {
                    read_timeout: 10,
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn test_redis_multi_write_opts() {
        let yaml = r#"
configs:
    - cluster_nodes:
          - "redis://127.0.0.1:6379"
          - "redis://127.0.0.2:6379"
      max_connections: 42
      connection_timeout: 5
    - server: "redis://127.0.0.1:6379"
      max_connections: 84
      connection_timeout: 10
    - configs:
          - server: "redis://127.0.0.1:6379"
            max_connections: 42
            connection_timeout: 5
"#;

        let config: RedisConfig = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis config: single with options");

        assert_eq!(
            config,
            RedisConfig::MultiWrite {
                configs: vec![
                    RedisConfig::Cluster {
                        cluster_nodes: vec![
                            "redis://127.0.0.1:6379".to_owned(),
                            "redis://127.0.0.2:6379".to_owned(),
                        ],
                        options: PartialRedisConfigOptions {
                            max_connections: Some(42),
                            connection_timeout: 5,
                            ..Default::default()
                        },
                    },
                    RedisConfig::Single {
                        server: "redis://127.0.0.1:6379".to_owned(),
                        options: PartialRedisConfigOptions {
                            max_connections: Some(84),
                            connection_timeout: 10,
                            ..Default::default()
                        },
                    },
                    RedisConfig::MultiWrite {
                        configs: vec![RedisConfig::Single {
                            server: "redis://127.0.0.1:6379".to_owned(),
                            options: PartialRedisConfigOptions {
                                max_connections: Some(42),
                                connection_timeout: 5,
                                ..Default::default()
                            },
                        }]
                    }
                ],
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
            RedisConfigs::Unified(RedisConfig::Cluster {
                cluster_nodes: vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned()
                ],
                options: PartialRedisConfigOptions {
                    read_timeout: 10,
                    ..Default::default()
                },
            })
        );
    }

    #[test]
    fn test_redis_cluster_serialize() {
        let config = RedisConfig::Cluster {
            cluster_nodes: vec![
                "redis://127.0.0.1:6379".to_owned(),
                "redis://127.0.0.2:6379".to_owned(),
            ],
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
        let configs = RedisConfigs::Unified(RedisConfig::Cluster {
            cluster_nodes: vec![
                "redis://127.0.0.1:6379".to_owned(),
                "redis://127.0.0.2:6379".to_owned(),
            ],
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
            project_configs: Box::new(RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    connection_timeout: 5,
                    ..Default::default()
                },
            }),
            cardinality: Box::new(RedisConfig::Single {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: Default::default(),
            }),
            quotas: Box::new(RedisConfig::MultiWrite {
                configs: vec![
                    RedisConfig::Cluster {
                        cluster_nodes: vec![
                            "redis://127.0.0.1:6379".to_owned(),
                            "redis://127.0.0.2:6379".to_owned(),
                        ],
                        options: PartialRedisConfigOptions {
                            max_connections: Some(84),
                            connection_timeout: 10,
                            ..Default::default()
                        },
                    },
                    RedisConfig::Single {
                        server: "redis://127.0.0.1:6379".to_owned(),
                        options: PartialRedisConfigOptions {
                            max_connections: Some(42),
                            connection_timeout: 5,
                            ..Default::default()
                        },
                    },
                ],
            }),
            misc: Box::new(RedisConfig::Cluster {
                cluster_nodes: vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ],
                options: PartialRedisConfigOptions {
                    max_connections: Some(84),
                    connection_timeout: 10,
                    ..Default::default()
                },
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
            "configs": [
              {
                "cluster_nodes": [
                  "redis://127.0.0.1:6379",
                  "redis://127.0.0.2:6379"
                ],
                "max_connections": 84,
                "connection_timeout": 10,
                "max_lifetime": 300,
                "idle_timeout": 60,
                "read_timeout": 3,
                "write_timeout": 3
              },
              {
                "server": "redis://127.0.0.1:6379",
                "max_connections": 42,
                "connection_timeout": 5,
                "max_lifetime": 300,
                "idle_timeout": 60,
                "read_timeout": 3,
                "write_timeout": 3
              }
            ]
          },
          "misc": {
            "cluster_nodes": [
              "redis://127.0.0.1:6379",
              "redis://127.0.0.2:6379"
            ],
            "max_connections": 84,
            "connection_timeout": 10,
            "max_lifetime": 300,
            "idle_timeout": 60,
            "read_timeout": 3,
            "write_timeout": 3
          }
        }
        "###);
    }
}
