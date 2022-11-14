use std::collections::BTreeMap;

use failure::Fail;
use serde::{Deserialize, Serialize};

/// Kafka configuration errors.
#[derive(Fail, Debug)]
pub enum ConfigError {
    /// The user referenced a kafka config name that does not exist.
    #[fail(display = "unknown kafka config name")]
    UnknownKafkaConfigName,
    /// The user did not configure 0 shard
    #[fail(display = "invalid kafka shard configuration: must have shard with index 0")]
    InvalidShard,
}

/// Define the topics over which Relay communicates with Sentry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
    /// Session health updates.
    Sessions,
    /// Any metric that is extracted from sessions.
    MetricsSessions,
    /// Any metric that is extracted from transactions.
    MetricsTransactions,
    /// Profiles
    Profiles,
    /// ReplayEvents, breadcrumb + session updates for replays
    ReplayEvents,
    /// ReplayRecordings, large blobs sent by the replay sdk
    ReplayRecordings,
}

impl KafkaTopic {
    /// Returns iterator over the variants of [`KafkaTopic`].
    /// It will have to be adjusted if the new variants are added.
    pub fn iter() -> std::slice::Iter<'static, KafkaTopic> {
        use KafkaTopic::*;
        static TOPICS: [KafkaTopic; 11] = [
            Events,
            Attachments,
            Transactions,
            Outcomes,
            OutcomesBilling,
            Sessions,
            MetricsSessions,
            MetricsTransactions,
            Profiles,
            ReplayEvents,
            ReplayRecordings,
        ];
        TOPICS.iter()
    }
}

/// Configuration for topics.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct TopicAssignments {
    /// Simple events topic name.
    pub events: TopicAssignment,
    /// Events with attachments topic name.
    pub attachments: TopicAssignment,
    /// Transaction events topic name.
    pub transactions: TopicAssignment,
    /// Outcomes topic name.
    pub outcomes: TopicAssignment,
    /// Outcomes topic name for billing critical outcomes. Defaults to the assignment of `outcomes`.
    pub outcomes_billing: Option<TopicAssignment>,
    /// Session health topic name.
    pub sessions: TopicAssignment,
    /// Default topic name for all aggregate metrics. Specialized topics for session-based and
    /// transaction-based metrics can be configured via `metrics_sessions` and
    /// `metrics_transactions` each.
    pub metrics: TopicAssignment,
    /// Topic name for metrics extracted from sessions. Defaults to the assignment of `metrics`.
    pub metrics_sessions: Option<TopicAssignment>,
    /// Topic name for metrics extracted from transactions. Defaults to the assignment of `metrics`.
    pub metrics_transactions: Option<TopicAssignment>,
    /// Stacktrace topic name
    pub profiles: TopicAssignment,
    /// Replay Events topic name.
    pub replay_events: TopicAssignment,
    /// Recordings topic name.
    pub replay_recordings: TopicAssignment,
}

impl TopicAssignments {
    /// Get a topic assignment by [`KafkaTopic`] value
    #[must_use]
    pub fn get(&self, kafka_topic: KafkaTopic) -> &TopicAssignment {
        match kafka_topic {
            KafkaTopic::Attachments => &self.attachments,
            KafkaTopic::Events => &self.events,
            KafkaTopic::Transactions => &self.transactions,
            KafkaTopic::Outcomes => &self.outcomes,
            KafkaTopic::OutcomesBilling => self.outcomes_billing.as_ref().unwrap_or(&self.outcomes),
            KafkaTopic::Sessions => &self.sessions,
            KafkaTopic::MetricsSessions => self.metrics_sessions.as_ref().unwrap_or(&self.metrics),
            KafkaTopic::MetricsTransactions => {
                self.metrics_transactions.as_ref().unwrap_or(&self.metrics)
            }
            KafkaTopic::Profiles => &self.profiles,
            KafkaTopic::ReplayEvents => &self.replay_events,
            KafkaTopic::ReplayRecordings => &self.replay_recordings,
        }
    }
}

impl Default for TopicAssignments {
    fn default() -> Self {
        Self {
            events: "ingest-events".to_owned().into(),
            attachments: "ingest-attachments".to_owned().into(),
            transactions: "ingest-transactions".to_owned().into(),
            outcomes: "outcomes".to_owned().into(),
            outcomes_billing: None,
            sessions: "ingest-sessions".to_owned().into(),
            metrics: "ingest-metrics".to_owned().into(),
            metrics_sessions: None,
            metrics_transactions: None,
            profiles: "profiles".to_owned().into(),
            replay_events: "ingest-replay-events".to_owned().into(),
            replay_recordings: "ingest-replay-recordings".to_owned().into(),
        }
    }
}

/// Configuration for a "logical" topic/datasink that Relay should forward data into.
///
/// Can be either a string containing the kafka topic name to produce into (using the default
/// `kafka_config`), or an object containing keys `topic_name` and `kafka_config_name` for using a
/// custom kafka cluster.
///
/// See documentation for `secondary_kafka_configs` for more information.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum TopicAssignment {
    /// String containing the kafka topic name. In this case the default kafka cluster configured
    /// in `kafka_config` will be used.
    Primary(String),
    /// Object containing topic name and string identifier of one of the clusters configured in
    /// `secondary_kafka_configs`. In this case that custom kafka config will be used to produce
    /// data to the given topic name.
    Secondary(KafkaTopicConfig),
    /// If we want to configure multiple kafka clusters, we can create a mapping of the
    /// range of logical shards to the kafka configuration.
    Sharded(Sharded),
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
}

/// Configuration for logical shards -> kafka configuration mapping.
/// The configuration for this should look like:
///     ```
///     metrics:
///        shards: 65000
///        mapping:
///          0:
///              name: "ingest-metrics-1"
///              config: "metrics_1"
///          25000:
///              name: "ingest-metrics-2"
///              config: "metrics_2"
///          45000:
///              name: "ingest-metrics-3"
///              config: "metrics_3"
///     ```
///
/// where the `shards` defines how many logical shards must be created, and `mapping`
/// describes the per-shard configuration. Index in the `mapping` is the initial inclusive
/// index of the shard and the range is last till the next index or the maximum shard defined in
/// the `shards` option. The first index must always start with 0.
#[derive(Serialize, Deserialize, Debug)]
pub struct Sharded {
    /// The number of shards used for this topic.
    shards: u64,
    /// The Kafka configuration assigned to the specific shard range.
    mapping: BTreeMap<u64, KafkaTopicConfig>,
}

/// Describes Kafka config, with all the parameters extracted, which will be used for creating the
/// kafka producer.
#[derive(Debug)]
pub enum KafkaConfig<'a> {
    /// Single config with Kafka parameters.
    Single {
        /// Kafka topic name.
        topic: KafkaTopic,
        /// Kafka parameters to create the kafka producer.
        params: KafkaParams<'a>,
    },

    /// The list of the Kafka configs with related shard configs.
    Sharded {
        /// Kafka topic name.
        topic: KafkaTopic,
        /// The maximum number of logical shards for this set of configs.
        shards: u64,
        /// The list of the sharded Kafka configs.
        configs: BTreeMap<u64, KafkaParams<'a>>,
    },
}

/// Sharded Kafka config
#[derive(Debug)]
pub struct KafkaParams<'a> {
    /// The topic name to use.
    pub topic_name: &'a str,
    /// The Kafka config name will be used to produce data.
    pub config_name: Option<&'a str>,
    /// Parameters for the Kafka producer configuration.
    pub params: &'a [KafkaConfigParam],
}

impl From<String> for TopicAssignment {
    fn from(topic_name: String) -> Self {
        Self::Primary(topic_name)
    }
}

impl TopicAssignment {
    /// Get the kafka config for the current topic assignment.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if the configuration for the current topic assignement is invalid.
    pub fn kafka_config<'a>(
        &'a self,
        topic: KafkaTopic,
        default_config: &'a Vec<KafkaConfigParam>,
        secondary_configs: &'a BTreeMap<String, Vec<KafkaConfigParam>>,
    ) -> Result<KafkaConfig<'_>, ConfigError> {
        let kafka_config = match self {
            Self::Primary(topic_name) => KafkaConfig::Single {
                topic,
                params: KafkaParams {
                    topic_name,
                    config_name: None,
                    params: default_config.as_slice(),
                },
            },
            Self::Secondary(KafkaTopicConfig {
                topic_name,
                kafka_config_name,
            }) => KafkaConfig::Single {
                topic,
                params: KafkaParams {
                    config_name: Some(kafka_config_name),
                    topic_name,
                    params: secondary_configs
                        .get(kafka_config_name)
                        .ok_or(ConfigError::UnknownKafkaConfigName)?,
                },
            },
            Self::Sharded(Sharded { shards, mapping }) => {
                // quick fail if the config does not contain shard 0
                if !mapping.contains_key(&0) {
                    return Err(ConfigError::InvalidShard);
                }
                let mut kafka_params = BTreeMap::new();
                for (shard, kafka_config) in mapping {
                    let config = KafkaParams {
                        topic_name: kafka_config.topic_name.as_str(),
                        config_name: Some(kafka_config.kafka_config_name.as_str()),
                        params: secondary_configs
                            .get(kafka_config.kafka_config_name.as_str())
                            .ok_or(ConfigError::UnknownKafkaConfigName)?,
                    };
                    kafka_params.insert(*shard, config);
                }
                KafkaConfig::Sharded {
                    topic,
                    shards: *shards,
                    configs: kafka_params,
                }
            }
        };

        Ok(kafka_config)
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
        let yaml = r###"
events: "ingest-events-kafka-topic"
profiles:
    name: "ingest-profiles"
    config: "profiles"
metrics:
  shards: 65000
  mapping:
      0:
          name: "ingest-metrics-1"
          config: "metrics_1"
      25000:
          name: "ingest-metrics-2"
          config: "metrics_2"
      45000:
          name: "ingest-metrics-3"
          config: "metrics_3"
"###;

        let def_config = vec![KafkaConfigParam {
            name: "test".to_string(),
            value: "test-value".to_string(),
        }];
        let mut second_config = BTreeMap::new();
        second_config.insert(
            "profiles".to_string(),
            vec![KafkaConfigParam {
                name: "test".to_string(),
                value: "test-value".to_string(),
            }],
        );
        second_config.insert(
            "metrics_1".to_string(),
            vec![KafkaConfigParam {
                name: "test".to_string(),
                value: "test-value".to_string(),
            }],
        );
        second_config.insert(
            "metrics_2".to_string(),
            vec![KafkaConfigParam {
                name: "test".to_string(),
                value: "test-value".to_string(),
            }],
        );
        second_config.insert(
            "metrics_3".to_string(),
            vec![KafkaConfigParam {
                name: "test".to_string(),
                value: "test-value".to_string(),
            }],
        );
        let topics: TopicAssignments = serde_yaml::from_str(yaml).unwrap();
        let events = topics.events;
        let profiles = topics.profiles;
        let metrics = topics.metrics;

        assert!(matches!(events, TopicAssignment::Primary(_)));
        assert!(matches!(profiles, TopicAssignment::Secondary { .. }));
        assert!(matches!(metrics, TopicAssignment::Sharded { .. }));

        let events_config = metrics
            .kafka_config(KafkaTopic::MetricsTransactions, &def_config, &second_config)
            .expect("Kafka config for metrics topic");
        assert!(matches!(events_config, KafkaConfig::Sharded { .. }));

        let events_config = events
            .kafka_config(KafkaTopic::Events, &def_config, &second_config)
            .expect("Kafka config for events topic");
        assert!(matches!(events_config, KafkaConfig::Single { .. }));

        let (shards, mapping) =
            if let TopicAssignment::Sharded(Sharded { shards, mapping }) = metrics {
                (shards, mapping)
            } else {
                unreachable!()
            };
        assert_eq!(shards, 65000);
        assert_eq!(3, mapping.len());
    }
}
