//! Configuration primitives to configure the kafka producer and properly set up the connection.
//!
//! The configuration can be either;
//! - [`TopicAssignment::Primary`] - the main and default kafka configuration,
//! - [`TopicAssignment::Secondary`] - used to configure any additional kafka topic,

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de};
use thiserror::Error;

use crate::utils;

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
        #[derive(Deserialize, Serialize, Debug)]
        #[serde(default)]
        pub struct TopicAssignments {
            $(
                #[serde(alias = $default_topic)]
                #[doc = $doc]
                pub $field_name: TopicAssignment,
            )*

            /// Additional topic assignments configured but currently unused by this Relay instance.
            #[serde(flatten, skip_serializing)]
            pub unused: Unused,
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
                    unused: Default::default()
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

/// A list of all currently, by this Relay, unused topic configurations.
#[derive(Debug, Default)]
pub struct Unused(Vec<String>);

impl Unused {
    /// Returns all unused topic names.
    pub fn names(&self) -> &[String] {
        &self.0
    }
}

impl<'de> de::Deserialize<'de> for Unused {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let topics = BTreeMap::<String, de::IgnoredAny>::deserialize(deserializer)?;
        Ok(Self(topics.into_keys().collect()))
    }
}

/// Configuration for a "logical" topic/datasink that Relay should forward data into.
///
/// Can be either a string containing the kafka topic name to produce into (using the default
/// `kafka_config`), an object containing keys `topic_name` and `kafka_config_name` for using a
/// custom kafka cluster, or an array of topic names/configs for sharded topics.
///
/// See documentation for `secondary_kafka_configs` for more information.
#[derive(Debug, Serialize)]
pub struct TopicAssignment(Vec<TopicConfig>);

impl<'de> de::Deserialize<'de> for TopicAssignment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Inner {
            Primary(String),
            Secondary(TopicConfig),
        }

        let configs = utils::one_or_many(deserializer)?
            .into_iter()
            .map(|inner| match inner {
                Inner::Primary(topic_name) => topic_name.into(),
                Inner::Secondary(config) => config,
            })
            .collect();

        Ok(Self(configs))
    }
}

/// Configuration for topic
#[derive(Debug, Deserialize, Serialize)]
pub struct TopicConfig {
    /// The topic name to use.
    #[serde(rename = "name")]
    topic_name: String,
    /// The Kafka config name will be used to produce data to the given topic.
    ///
    /// If the config is missing, the default config will be used.
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    kafka_config_name: Option<String>,
    /// Optionally, a rate limit per partition key to protect against partition imbalance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_rate_limit: Option<KeyRateLimit>,
}

impl From<String> for TopicConfig {
    fn from(topic_name: String) -> Self {
        Self {
            topic_name,
            kafka_config_name: None,
            key_rate_limit: None,
        }
    }
}

/// Produce rate limit configuration for a topic.
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub struct KeyRateLimit {
    /// Limit each partition key to N messages per `window_secs`.
    pub limit_per_window: u64,

    /// The size of the window to record counters for.
    ///
    /// Larger windows imply higher memory usage.
    pub window_secs: u64,
}

/// A Kafka config for a topic.
///
/// This internally includes configuration for multiple 'physical' Kafka topics,
/// as Relay can shard to multiple topics at once.
#[derive(Debug)]
pub struct KafkaTopicConfig<'a>(Vec<KafkaParams<'a>>);

impl<'a> KafkaTopicConfig<'a> {
    pub fn topics(&self) -> &[KafkaParams<'a>] {
        &self.0
    }
}

/// Config for creating a Kafka producer.
#[derive(Debug)]
pub struct KafkaParams<'a> {
    /// The topic names to use. Can be a single topic or multiple topics for sharding.
    pub topic_name: String,
    /// The Kafka config name will be used to produce data.
    pub config_name: Option<&'a str>,
    /// Parameters for the Kafka producer configuration.
    pub params: &'a [KafkaConfigParam],
    /// Optionally, a rate limit per partition key to protect against partition imbalance.
    pub key_rate_limit: Option<KeyRateLimit>,
}

impl From<String> for TopicAssignment {
    fn from(topic_name: String) -> Self {
        Self(vec![topic_name.into()])
    }
}

impl TopicAssignment {
    /// Get the Kafka configs for the current topic assignment.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if the configuration for the current topic assignment is invalid.
    pub fn kafka_configs<'a>(
        &'a self,
        default_config: &'a Vec<KafkaConfigParam>,
        secondary_configs: &'a BTreeMap<String, Vec<KafkaConfigParam>>,
    ) -> Result<KafkaTopicConfig<'a>, ConfigError> {
        let configs = self
            .0
            .iter()
            .map(|tc| {
                Ok(KafkaParams {
                    topic_name: tc.topic_name.clone(),
                    config_name: tc.kafka_config_name.as_deref(),
                    params: match &tc.kafka_config_name {
                        Some(config) => secondary_configs
                            .get(config)
                            .ok_or(ConfigError::UnknownKafkaConfigName)?,
                        None => default_config.as_slice(),
                    },
                    key_rate_limit: tc.key_rate_limit,
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(KafkaTopicConfig(configs))
    }
}

/// A name value pair of Kafka config parameter.
#[derive(Debug, Deserialize, Serialize)]
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
        insta::assert_debug_snapshot!(topics, @"");
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

        let events_configs = topics
            .events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for sharded events topic");

        insta::assert_debug_snapshot!(events_configs, @"");

        let profiles_configs = topics
            .profiles
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for sharded profiles topic");

        insta::assert_debug_snapshot!(profiles_configs, @"");
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

        let events_configs = topics
            .events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for mixed broker sharded topic");

        insta::assert_debug_snapshot!(events_configs, @"")
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

        let events_configs = topics
            .events
            .kafka_configs(&def_config, &second_config)
            .expect("Kafka config for per-shard rate limits");

        insta::assert_debug_snapshot!(events_configs, @"");
    }
}
