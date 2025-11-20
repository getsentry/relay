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
    /// Sets the idle timeout used by the pool, in seconds.
    ///
    /// The idle timeout defines the maximum time a connection will be kept in the pool if unused.
    pub idle_timeout: u64,
    /// Sets the maximum time in seconds to wait when establishing a new Redis connection.
    ///
    /// If a connection cannot be established within this duration, it is considered a failure.
    /// Applies when the pool needs to grow or create fresh connections.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_timeout: Option<u64>,
    /// Sets the maximum time in seconds to validate an existing connection when it is recycled.
    ///
    /// Recycling involves checking whether an idle connection is still alive before reuse.
    /// If validation exceeds this timeout, the connection is discarded and a new fetch from the pool
    /// is attempted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recycle_timeout: Option<u64>,
    /// Sets the maximum time, in seconds, that a caller is allowed to wait
    /// when requesting a connection from the pool.
    ///
    /// If a connection does not become available within this period, the attempt
    /// will fail with a timeout error. This setting helps prevent indefinite
    /// blocking when the pool is exhausted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<u64>,
    /// Sets the maximum time in seconds to wait for a result when sending a Redis command.
    ///
    /// If a command exceeds this timeout, the connection will be recycled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_timeout: Option<u64>,
    /// Sets the number of times after which the connection will check whether it is active when
    /// being recycled.
    ///
    /// A frequency of 1, means that the connection will check whether it is active every time it
    /// is recycled.
    pub recycle_check_frequency: usize,
}

impl Default for PartialRedisConfigOptions {
    fn default() -> Self {
        Self {
            max_connections: None,
            idle_timeout: 60,
            create_timeout: Some(3),
            recycle_timeout: Some(2),
            wait_timeout: None,
            response_timeout: Some(30),
            recycle_check_frequency: 100,
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
    /// Connect to a single Redis instance.
    Single(SingleRedisConfig),
}

/// Struct that can serialize a string to a single Redis connection.
///
/// This struct is needed for backward compatibility.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum SingleRedisConfig {
    #[doc(hidden)]
    Simple(String),
    #[doc(hidden)]
    Detailed {
        #[doc(hidden)]
        server: String,
        #[doc(hidden)]
        #[serde(flatten)]
        options: PartialRedisConfigOptions,
    },
}

impl RedisConfig {
    /// Creates a new Redis config for a single Redis instance with default settings.
    pub fn single(server: String) -> Self {
        RedisConfig::Single(SingleRedisConfig::Detailed {
            server,
            options: Default::default(),
        })
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
            RedisConfigFromFile::Single(server) => Self::Single(SingleRedisConfig::Detailed {
                server,
                options: Default::default(),
            }),
            RedisConfigFromFile::SingleWithOpts { server, options } => {
                Self::Single(SingleRedisConfig::Detailed { server, options })
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
    /// Connect to a single Redis instance.
    Single {
        /// Reference to the Redis node url.
        server: &'a String,
        /// Options of the Redis config.
        options: RedisConfigOptions,
    },
}

/// Helper struct bundling connections and options for the various Redis pools.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum RedisConfigsRef<'a> {
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
    },
}

fn build_redis_config_options(
    options: &PartialRedisConfigOptions,
    default_connections: u32,
) -> RedisConfigOptions {
    let max_connections = options.max_connections.unwrap_or(default_connections);

    RedisConfigOptions {
        max_connections,
        idle_timeout: options.idle_timeout,
        create_timeout: options.create_timeout,
        recycle_timeout: options.recycle_timeout,
        wait_timeout: options.wait_timeout,
        response_timeout: options.response_timeout,
        recycle_check_frequency: options.recycle_check_frequency,
    }
}

/// Builds a [`RedisConfigsRef`] given a [`RedisConfig`].
///
/// The returned config contains more options for setting up Redis.
pub(super) fn build_redis_config(
    config: &RedisConfig,
    default_connections: u32,
) -> RedisConfigRef<'_> {
    match config {
        RedisConfig::Cluster {
            cluster_nodes,
            options,
        } => RedisConfigRef::Cluster {
            cluster_nodes,
            options: build_redis_config_options(options, default_connections),
        },
        RedisConfig::Single(SingleRedisConfig::Detailed { server, options }) => {
            RedisConfigRef::Single {
                server,
                options: build_redis_config_options(options, default_connections),
            }
        }
        RedisConfig::Single(SingleRedisConfig::Simple(server)) => RedisConfigRef::Single {
            server,
            options: Default::default(),
        },
    }
}

/// Builds a [`RedisConfigsRef`] given a [`RedisConfigs`].
///
/// The returned configs contain more options for setting up Redis.
pub(super) fn build_redis_configs(
    configs: &RedisConfigs,
    cpu_concurrency: u32,
    pool_concurrency: u32,
) -> RedisConfigsRef<'_> {
    // Project configurations are estimated to need around or less of `cpu_concurrency`
    // connections, double this value for some extra headroom.
    //
    // For smaller setups give some extra headroom through `DEFAULT_MIN_MAX_CONNECTIONS`.
    let project_connections = std::cmp::max(cpu_concurrency * 2, DEFAULT_MIN_MAX_CONNECTIONS);
    // The total concurrency for rate limiting/processing is the total pool concurrency
    // calculated by `cpu_concurrency * pool_concurrency`.
    //
    // No need to consider `DEFAULT_MIN_MAX_CONNECTIONS`, as these numbers are accurate.
    let total_pool_concurrency = cpu_concurrency * pool_concurrency;

    match configs {
        RedisConfigs::Unified(cfg) => {
            // A unified pool needs enough connections for project configs and enough to satisfy
            // the processing pool.
            let default_connections = total_pool_concurrency + project_connections;
            let config = build_redis_config(cfg, default_connections);
            RedisConfigsRef::Unified(config)
        }
        RedisConfigs::Individual {
            project_configs,
            cardinality,
            quotas,
        } => {
            let project_configs = build_redis_config(project_configs, project_connections);
            let cardinality = build_redis_config(cardinality, total_pool_concurrency);
            let quotas = build_redis_config(quotas, total_pool_concurrency);
            RedisConfigsRef::Individual {
                project_configs,
                cardinality,
                quotas,
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
            RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    ..Default::default()
                }
            })
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
            RedisConfigs::Unified(RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    ..Default::default()
                }
            }))
        );
    }

    #[test]
    fn test_redis_individual() {
        let yaml = r#"
project_configs:
    server: "redis://127.0.0.1:6379"
    max_connections: 42
cardinality:
    server: "redis://127.0.0.1:6379"
quotas:
    cluster_nodes:
        - "redis://127.0.0.1:6379"
        - "redis://127.0.0.2:6379"
    max_connections: 17
"#;

        let configs: RedisConfigs = serde_yaml::from_str(yaml)
            .expect("Parsed processing redis configs: single with options");

        let expected = RedisConfigs::Individual {
            project_configs: Box::new(RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    ..Default::default()
                },
            })),
            cardinality: Box::new(RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: Default::default(),
            })),
            quotas: Box::new(RedisConfig::Cluster {
                cluster_nodes: vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ],
                options: PartialRedisConfigOptions {
                    max_connections: Some(17),
                    ..Default::default()
                },
            }),
        };

        assert_eq!(configs, expected);
    }

    #[test]
    fn test_redis_single_serialize() {
        let config = RedisConfig::Single(SingleRedisConfig::Detailed {
            server: "redis://127.0.0.1:6379".to_owned(),
            options: PartialRedisConfigOptions {
                max_connections: Some(42),
                ..Default::default()
            },
        });

        assert_json_snapshot!(config, @r###"
        {
          "server": "redis://127.0.0.1:6379",
          "max_connections": 42,
          "idle_timeout": 60,
          "create_timeout": 3,
          "recycle_timeout": 2,
          "response_timeout": 30,
          "recycle_check_frequency": 100
        }
        "###);
    }

    #[test]
    fn test_redis_single_serialize_unified() {
        let configs = RedisConfigs::Unified(RedisConfig::Single(SingleRedisConfig::Detailed {
            server: "redis://127.0.0.1:6379".to_owned(),
            options: PartialRedisConfigOptions {
                max_connections: Some(42),
                ..Default::default()
            },
        }));

        assert_json_snapshot!(configs, @r###"
        {
          "server": "redis://127.0.0.1:6379",
          "max_connections": 42,
          "idle_timeout": 60,
          "create_timeout": 3,
          "recycle_timeout": 2,
          "response_timeout": 30,
          "recycle_check_frequency": 100
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
            RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: Default::default()
            })
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
            RedisConfig::Single(SingleRedisConfig::Simple(
                "redis://127.0.0.1:6379".to_owned()
            ))
        );
    }

    #[test]
    fn test_redis_cluster_nodes_opts() {
        let yaml = r#"
cluster_nodes:
    - "redis://127.0.0.1:6379"
    - "redis://127.0.0.2:6379"
max_connections: 10
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
                    max_connections: Some(10),
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
max_connections: 20
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
                    max_connections: Some(20),
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
                max_connections: Some(42),
                ..Default::default()
            },
        };

        assert_json_snapshot!(config, @r###"
        {
          "cluster_nodes": [
            "redis://127.0.0.1:6379",
            "redis://127.0.0.2:6379"
          ],
          "max_connections": 42,
          "idle_timeout": 60,
          "create_timeout": 3,
          "recycle_timeout": 2,
          "response_timeout": 30,
          "recycle_check_frequency": 100
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
                max_connections: Some(42),
                ..Default::default()
            },
        });

        assert_json_snapshot!(configs, @r###"
        {
          "cluster_nodes": [
            "redis://127.0.0.1:6379",
            "redis://127.0.0.2:6379"
          ],
          "max_connections": 42,
          "idle_timeout": 60,
          "create_timeout": 3,
          "recycle_timeout": 2,
          "response_timeout": 30,
          "recycle_check_frequency": 100
        }
        "###);
    }

    #[test]
    fn test_redis_serialize_individual() {
        let configs = RedisConfigs::Individual {
            project_configs: Box::new(RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: PartialRedisConfigOptions {
                    max_connections: Some(42),
                    ..Default::default()
                },
            })),
            cardinality: Box::new(RedisConfig::Single(SingleRedisConfig::Detailed {
                server: "redis://127.0.0.1:6379".to_owned(),
                options: Default::default(),
            })),
            quotas: Box::new(RedisConfig::Cluster {
                cluster_nodes: vec![
                    "redis://127.0.0.1:6379".to_owned(),
                    "redis://127.0.0.2:6379".to_owned(),
                ],
                options: PartialRedisConfigOptions {
                    max_connections: Some(84),
                    ..Default::default()
                },
            }),
        };

        assert_json_snapshot!(configs, @r###"
        {
          "project_configs": {
            "server": "redis://127.0.0.1:6379",
            "max_connections": 42,
            "idle_timeout": 60,
            "create_timeout": 3,
            "recycle_timeout": 2,
            "response_timeout": 30,
            "recycle_check_frequency": 100
          },
          "cardinality": {
            "server": "redis://127.0.0.1:6379",
            "idle_timeout": 60,
            "create_timeout": 3,
            "recycle_timeout": 2,
            "response_timeout": 30,
            "recycle_check_frequency": 100
          },
          "quotas": {
            "cluster_nodes": [
              "redis://127.0.0.1:6379",
              "redis://127.0.0.2:6379"
            ],
            "max_connections": 84,
            "idle_timeout": 60,
            "create_timeout": 3,
            "recycle_timeout": 2,
            "response_timeout": 30,
            "recycle_check_frequency": 100
          }
        }
        "###);
    }
}
