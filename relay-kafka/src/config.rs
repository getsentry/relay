//! Configuration primitives to configure the kafka producer and properly set up the connection.
//!
//! The configuration can be either;
//! - [`TopicAssignment::Primary`] - the main and default kafka configuration,
//! - [`TopicAssignment::Secondary`] - used to configure any additional kafka topic,

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Kafka configuration errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// The user referenced a kafka config name that does not exist.
    #[error("unknown kafka config name")]
    UnknownKafkaConfigName,
    /// The user did not configure 0 shard
    #[error("invalid kafka shard configuration: must have shard with index 0")]
    InvalidShard,
}

/// Define the topics over which Relay communicates with Sentry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum KafkaTopic {
    /// Simple events (without attachments) topic.
    Events,
    /// Complex events (with attachments) topic.
    Attachments,
    /// Transaction events topic.
    Transactions,
    /// Shared outcomes topic for Relay and Sentry.
    Outcomes,
    /// Override for billing critical outcomes.
    OutcomesBilling,
    /// Any metric that is extracted from sessions.
    MetricsSessions,
    /// Generic metrics topic, excluding sessions (release health).
    MetricsGeneric,
    /// Profiles
    Profiles,
    /// ReplayEvents, breadcrumb + session updates for replays
    ReplayEvents,
    /// ReplayRecordings, large blobs sent by the replay sdk
    ReplayRecordings,
    /// Monitor check-ins.
    Monitors,
    /// Logs (our log product).
    OurLogs,
    /// Standalone spans without a transaction.
    Spans,
    /// Feedback events topic.
    Feedback,
    /// Items topic
    Items,
}

impl KafkaTopic {
    /// Returns iterator over the variants of [`KafkaTopic`].
    /// It will have to be adjusted if the new variants are added.
    pub fn iter() -> std::slice::Iter<'static, Self> {
        use KafkaTopic::*;
        static TOPICS: [KafkaTopic; 15] = [
            Events,
            Attachments,
            Transactions,
            Outcomes,
            OutcomesBilling,
            MetricsSessions,
            MetricsGeneric,
            Profiles,
            ReplayEvents,
            ReplayRecordings,
            Monitors,
            OurLogs,
            Spans,
            Feedback,
            Items,
        ];
        TOPICS.iter()
    }
}

macro_rules! define_topic_assignments {
    ($($field_name:ident : ($kafka_topic:path, $default_topic:literal, $doc:literal)),* $(,)?) => {
        /// Configuration for topics.
        #[derive(Serialize, Deserialize, Debug)]
        #[serde(default)]
        pub struct TopicAssignments {
            $(
                #[serde(alias = $default_topic)]
                #[doc = $doc]
                pub $field_name: TopicAssignment,
            )*

            /// Additional topic assignments configured but currently unused by this Relay instance.
            #[serde(flatten)]
            pub unused: BTreeMap<String, TopicAssignment>,
        }

        impl TopicAssignments{
            /// Get a topic assignment by [`KafkaTopic`] value
            #[must_use]
            pub fn get(&self, kafka_topic: KafkaTopic) -> &TopicAssignment {
                match kafka_topic {
                    $(
                        $kafka_topic => &self.$field_name,
                    )*
                }
            }
        }

        impl Default for TopicAssignments {
            fn default() -> Self {
                Self {
                    $(
                        $field_name: $default_topic.to_owned().into(),
                    )*
                    unused: BTreeMap::new(),
                }
            }
        }
    };
}

// WARNING: When adding a topic here, make sure that the kafka topic exists or can be auto-created.
// Failure to do so will result in Relay crashing (if the `kafka_validate_topics` config flag is enabled),
// or event loss in the store service.
define_topic_assignments! {
    events: (KafkaTopic::Events, "ingest-events", "Simple events topic name."),
    attachments: (KafkaTopic::Attachments, "ingest-attachments", "Events with attachments topic name."),
    transactions: (KafkaTopic::Transactions, "ingest-transactions", "Transaction events topic name."),
    outcomes: (KafkaTopic::Outcomes, "outcomes", "Outcomes topic name."),
    outcomes_billing: (KafkaTopic::OutcomesBilling, "outcomes-billing", "Outcomes topic name for billing critical outcomes."),
    metrics_sessions: (KafkaTopic::MetricsSessions, "ingest-metrics", "Topic name for metrics extracted from sessions, aka release health."),
    metrics_generic: (KafkaTopic::MetricsGeneric, "ingest-performance-metrics", "Topic name for all other kinds of metrics."),
    profiles: (KafkaTopic::Profiles, "profiles", "Stacktrace topic name"),
    replay_events: (KafkaTopic::ReplayEvents, "ingest-replay-events", "Replay Events topic name."),
    replay_recordings: (KafkaTopic::ReplayRecordings, "ingest-replay-recordings", "Recordings topic name."),
    ourlogs: (KafkaTopic::OurLogs, "snuba-ourlogs", "Logs from our logs product."),
    monitors: (KafkaTopic::Monitors, "ingest-monitors", "Monitor check-ins."),
    spans: (KafkaTopic::Spans, "snuba-spans", "Standalone spans without a transaction."),
    feedback: (KafkaTopic::Feedback, "ingest-feedback-events", "Feedback events topic."),
    items: (KafkaTopic::Items, "snuba-items", "Items topic."),
}

/// Configuration for a "logical" topic/datasink that Relay should forward data into.
///
/// Can be either a string containing the kafka topic name to produce into (using the default
/// `kafka_config`), an object containing keys `topic_name` and `kafka_config_name` for using a
/// custom kafka cluster, or an array of topic names/configs for sharded topics.
///
/// See documentation for `secondary_kafka_configs` for more information.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum TopicAssignment {
    /// String containing the kafka topic name. In this case the default kafka cluster configured
    /// in `kafka_config` will be used.
    Primary(String),
    /// Array of topic configs for sharded topics. Messages will be distributed across
    /// these topics based on partition key or randomly if no key is available.
    ///
    /// The order of shards in this array is critical for consistent hash-based routing. Changing
    /// the order will cause messages to be routed to different shards, breaking semantic
    /// partitioning. Appending new shards also breaks semantic partitioning.
    Sharded(Vec<ShardConfig>),
    /// Object containing topic name and string identifier of one of the clusters configured in
    /// `secondary_kafka_configs`. In this case that custom kafka config will be used to produce
    /// data to the given topic name.
    Secondary(KafkaTopicConfig),
}

/// Configuration for a shard within a sharded topic assignment.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ShardConfig {
    /// String containing the kafka topic name. Uses the default kafka cluster.
    Primary(String),
    /// Object containing topic name and kafka config for custom cluster.
    Secondary(KafkaTopicConfig),
}

/// Configuration for topic
#[derive(Serialize, Deserialize, Debug)]
pub struct KafkaTopicConfig {
    /// The topic name to use.
    #[serde(rename = "name")]
    topic_name: String,
    /// The Kafka config name will be used to produce data to the given topic.
    #[serde(rename = "config")]
    kafka_config_name: String,
    /// Optionally, a rate limit per partition key to protect against partition imbalance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_rate_limit: Option<KeyRateLimit>,
}

/// Produce rate limit configuration for a topic.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct KeyRateLimit {
    /// Limit each partition key to N messages per `window_secs`.
    pub limit_per_window: u64,

    /// The size of the window to record counters for.
    ///
    /// Larger windows imply higher memory usage.
    pub window_secs: u64,
}

/// Config for creating a Kafka producer.
#[derive(Debug)]
pub struct KafkaParams<'a> {
    /// The topic names to use. Can be a single topic or multiple topics for sharding.
    pub topic_names: Vec<String>,
    /// The Kafka config name will be used to produce data.
    pub config_name: Option<&'a str>,
    /// Parameters for the Kafka producer configuration.
    pub params: &'a [KafkaConfigParam],
    /// Optionally, a rate limit per partition key to protect against partition imbalance.
    pub key_rate_limit: Option<KeyRateLimit>,
}

impl From<String> for TopicAssignment {
    fn from(topic_name: String) -> Self {
        Self::Primary(topic_name)
    }
}

impl TopicAssignment {
    /// Get the kafka configs for the current topic assignment.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if the configuration for the current topic assignment is invalid.
    pub fn kafka_configs<'a>(
        &'a self,
        default_config: &'a Vec<KafkaConfigParam>,
        secondary_configs: &'a BTreeMap<String, Vec<KafkaConfigParam>>,
    ) -> Result<Vec<KafkaParams<'a>>, ConfigError> {
        let kafka_configs = match self {
            Self::Primary(topic_name) => vec![KafkaParams {
                topic_names: vec![topic_name.clone()],
                config_name: None,
                params: default_config.as_slice(),
                // XXX: Rate limits can only be set if the non-default kafka broker config is used,
                // i.e. in the Secondary codepath
                key_rate_limit: None,
            }],
            Self::Sharded(shard_configs) => {
                if shard_configs.is_empty() {
                    return Err(ConfigError::InvalidShard);
                }

                shard_configs
                    .iter()
                    .map(|shard_config| match shard_config {
                        ShardConfig::Primary(topic_name) => Ok(KafkaParams {
                            topic_names: vec![topic_name.clone()],
                            config_name: None,
                            params: default_config.as_slice(),
                            key_rate_limit: None,
                        }),
                        ShardConfig::Secondary(KafkaTopicConfig {
                            topic_name,
                            kafka_config_name,
                            key_rate_limit,
                        }) => {
                            let params = secondary_configs
                                .get(kafka_config_name)
                                .ok_or(ConfigError::UnknownKafkaConfigName)?;

                            Ok(KafkaParams {
                                topic_names: vec![topic_name.clone()],
                                config_name: Some(kafka_config_name.as_str()),
                                params: params.as_slice(),
                                key_rate_limit: *key_rate_limit,
                            })
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            Self::Secondary(KafkaTopicConfig {
                topic_name,
                kafka_config_name,
                key_rate_limit,
            }) => vec![KafkaParams {
                topic_names: vec![topic_name.clone()],
                config_name: Some(kafka_config_name),
                params: secondary_configs
                    .get(kafka_config_name)
                    .ok_or(ConfigError::UnknownKafkaConfigName)?,
                key_rate_limit: *key_rate_limit,
            }],
        };

        Ok(kafka_configs)
    }
}

/// A name value pair of Kafka config parameter.
#[derive(Serialize, Deserialize, Debug)]
pub struct KafkaConfigParam {
    /// Name of the Kafka config parameter.
    pub name: String,
    /// Value of the Kafka config parameter.
    pub value: String,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_kafka_config() {
        let yaml = r#"
ingest-events: "ingest-events-kafka-topic"
profiles:
    name: "ingest-profiles"
    config: "profiles"
ingest-metrics: "ingest-metrics-3"
transactions: "ingest-transactions-kafka-topic"
"#;

        let def_config = vec![KafkaConfigParam {
            name: "test".to_owned(),
            value: "test-value".to_owned(),
        }];
        let mut second_config = BTreeMap::new();
        second_config.insert(
            "profiles".to_owned(),
            vec![KafkaConfigParam {
                name: "test".to_owned(),
                value: "test-value".to_owned(),
            }],
        );

        let topics: TopicAssignments = serde_yaml::from_str(yaml).unwrap();
        let events = topics.events;
        let profiles = topics.profiles;
        let metrics_sessions = topics.metrics_sessions;
        let transactions = topics.transactions;

        assert!(matches!(events, TopicAssignment::Primary(_)));
        assert!(matches!(profiles, TopicAssignment::Secondary { .. }));
        assert!(matches!(metrics_sessions, TopicAssignment::Primary(_)));
        assert!(matches!(transactions, TopicAssignment::Primary(_)));

        let events_configs = events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for events topic");
        assert_eq!(events_configs.len(), 1);
        assert_eq!(
            events_configs[0].topic_names,
            vec!["ingest-events-kafka-topic"]
        );

        let profiles_configs = profiles
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for profiles topic");
        assert_eq!(profiles_configs.len(), 1);
        assert_eq!(profiles_configs[0].topic_names, vec!["ingest-profiles"]);
        assert_eq!(profiles_configs[0].config_name, Some("profiles"));

        let metrics_configs = metrics_sessions
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for metrics topic");
        assert_eq!(metrics_configs.len(), 1);
        assert_eq!(metrics_configs[0].topic_names, vec!["ingest-metrics-3"]);

        // Legacy keys are still supported
        let transactions_configs = transactions
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for transactions topic");
        assert_eq!(transactions_configs.len(), 1);
        assert_eq!(
            transactions_configs[0].topic_names,
            vec!["ingest-transactions-kafka-topic"]
        );
    }

    #[test]
    fn test_default_topic_is_valid() {
        let topic_assignments = TopicAssignments::default();

        // A few topics are not defined currently, remove this once added to `sentry-kafka-schemas`.
        let currrently_undefined_topics = [
            "ingest-attachments".to_owned(),
            "ingest-transactions".to_owned(),
            "profiles".to_owned(),
            "ingest-monitors".to_owned(),
        ];

        for topic in KafkaTopic::iter() {
            match topic_assignments.get(*topic) {
                TopicAssignment::Primary(logical_topic_name) => {
                    if !currrently_undefined_topics.contains(logical_topic_name) {
                        assert!(sentry_kafka_schemas::get_schema(logical_topic_name, None).is_ok());
                    }
                }
                _ => panic!("invalid default"),
            }
        }
    }

    #[test]
    fn test_sharded_kafka_config() {
        let yaml = r#"
events: ["ingest-events-1", "ingest-events-2"]
profiles:
  - name: "ingest-profiles-1"
    config: "profiles"
  - name: "ingest-profiles-2"
    config: "profiles"
"#;

        let def_config = vec![KafkaConfigParam {
            name: "test".to_owned(),
            value: "test-value".to_owned(),
        }];
        let mut second_config = BTreeMap::new();
        second_config.insert(
            "profiles".to_owned(),
            vec![KafkaConfigParam {
                name: "test".to_owned(),
                value: "test-value".to_owned(),
            }],
        );

        let topics: TopicAssignments = serde_yaml::from_str(yaml).unwrap();
        let events = topics.events;
        let profiles = topics.profiles;

        assert!(matches!(events, TopicAssignment::Sharded(_)));
        assert!(matches!(profiles, TopicAssignment::Sharded(_)));

        let events_configs = events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for sharded events topic");

        assert_eq!(events_configs.len(), 2); // One config per shard
        assert_eq!(events_configs[0].topic_names, vec!["ingest-events-1"]);
        assert_eq!(events_configs[1].topic_names, vec!["ingest-events-2"]);
        assert_eq!(events_configs[0].config_name, None); // Both use default config
        assert_eq!(events_configs[1].config_name, None);

        let profiles_configs = profiles
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for sharded profiles topic");

        assert_eq!(profiles_configs.len(), 2); // One config per shard
        assert_eq!(profiles_configs[0].topic_names, vec!["ingest-profiles-1"]);
        assert_eq!(profiles_configs[1].topic_names, vec!["ingest-profiles-2"]);
        assert_eq!(profiles_configs[0].config_name, Some("profiles")); // Both use profiles config
        assert_eq!(profiles_configs[1].config_name, Some("profiles"));
    }

    #[test]
    fn test_mixed_broker_sharded_config() {
        let yaml = r#"
events:
  - "ingest-events-1"
  - name: "ingest-events-2"
    config: "secondary"
"#;

        let def_config = vec![KafkaConfigParam {
            name: "bootstrap.servers".to_owned(),
            value: "primary:9092".to_owned(),
        }];
        let mut second_config = BTreeMap::new();
        second_config.insert(
            "secondary".to_owned(),
            vec![KafkaConfigParam {
                name: "bootstrap.servers".to_owned(),
                value: "secondary:9092".to_owned(),
            }],
        );

        let topics: TopicAssignments = serde_yaml::from_str(yaml).unwrap();
        let events = topics.events;

        assert!(matches!(events, TopicAssignment::Sharded(_)));

        // Mixed configs now return separate KafkaParams for each broker
        let events_configs = events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for mixed broker sharded topic");

        assert_eq!(events_configs.len(), 2); // Two different broker configs

        // Find the configs by checking config_name
        let primary_config = events_configs
            .iter()
            .find(|c| c.config_name.is_none())
            .unwrap();
        let secondary_config = events_configs
            .iter()
            .find(|c| c.config_name == Some("secondary"))
            .unwrap();

        assert_eq!(primary_config.topic_names, vec!["ingest-events-1"]);
        assert_eq!(secondary_config.topic_names, vec!["ingest-events-2"]);
        assert_eq!(secondary_config.config_name, Some("secondary"));
    }

    #[test]
    fn test_per_shard_rate_limits() {
        let yaml = r#"
events:
  - name: "shard-0"
    config: "cluster1"
    key_rate_limit:
      limit_per_window: 100
      window_secs: 60
  - name: "shard-1"
    config: "cluster2"
    key_rate_limit:
      limit_per_window: 200
      window_secs: 120
  - "shard-2"  # No rate limit (Primary variant)
"#;

        let def_config = vec![KafkaConfigParam {
            name: "bootstrap.servers".to_owned(),
            value: "primary:9092".to_owned(),
        }];
        let mut second_config = BTreeMap::new();
        second_config.insert(
            "cluster1".to_owned(),
            vec![KafkaConfigParam {
                name: "bootstrap.servers".to_owned(),
                value: "cluster1:9092".to_owned(),
            }],
        );
        second_config.insert(
            "cluster2".to_owned(),
            vec![KafkaConfigParam {
                name: "bootstrap.servers".to_owned(),
                value: "cluster2:9092".to_owned(),
            }],
        );

        let topics: TopicAssignments = serde_yaml::from_str(yaml).unwrap();
        let events = topics.events;

        let events_configs = events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for per-shard rate limits");

        assert_eq!(events_configs.len(), 3);

        // Shard 0: rate limit 100/60s with cluster1
        assert_eq!(events_configs[0].topic_names[0], "shard-0");
        assert_eq!(events_configs[0].config_name, Some("cluster1"));
        assert!(events_configs[0].key_rate_limit.is_some());
        let rate_limit_0 = events_configs[0].key_rate_limit.unwrap();
        assert_eq!(rate_limit_0.limit_per_window, 100);
        assert_eq!(rate_limit_0.window_secs, 60);

        // Shard 1: rate limit 200/120s with custom broker
        assert_eq!(events_configs[1].topic_names[0], "shard-1");
        assert_eq!(events_configs[1].config_name, Some("cluster2"));
        assert!(events_configs[1].key_rate_limit.is_some());
        let rate_limit_1 = events_configs[1].key_rate_limit.unwrap();
        assert_eq!(rate_limit_1.limit_per_window, 200);
        assert_eq!(rate_limit_1.window_secs, 120);

        // Shard 2: no rate limit
        assert_eq!(events_configs[2].topic_names[0], "shard-2");
        assert_eq!(events_configs[2].config_name, None);
        assert!(events_configs[2].key_rate_limit.is_none());
    }
}
