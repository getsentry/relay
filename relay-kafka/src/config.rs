//! Configuration primitives to configure the kafka producer and properly set up the connection.
//!
//! The configuration can be either;
//! - [`TopicAssignment::Primary`] - the main and default kafka configuration,
//! - [`TopicAssignment::Secondary`] - used to configure any additional kafka topic,
//! - [`TopicAssignment::Sharded`] - if we want to configure multiple kafka clusters,
//! we can create a mapping of the range of logical shards to the kafka configuration.

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
    /// Standalone spans without a transaction.
    Spans,
    /// Summary for metrics collected during a span.
    MetricsSummaries,
    /// COGS measurements topic.
    Cogs,
    /// Feedback events topic.
    Feedback,
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
            Spans,
            MetricsSummaries,
            Cogs,
            Feedback,
        ];
        TOPICS.iter()
    }
}

macro_rules! define_topic_assignments {
    ($struct_name:ident {
        $($field_name:ident : ($kafka_topic:path, $default_topic: literal, $doc: literal)),* $(,)? }) => {

        /// Configuration for topics.
        #[derive(Serialize, Deserialize, Debug)]
        #[serde(default)]
        pub struct TopicAssignments {
            $(
                #[serde(alias = $default_topic)]
                #[doc=$doc]
                pub $field_name: TopicAssignment,
            )*
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

                }
            }
        }
    };
}

define_topic_assignments! {
    TopicAssignments {
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
        monitors: (KafkaTopic::Monitors, "ingest-monitors", "Monitor check-ins."),
        spans: (KafkaTopic::Spans, "snuba-spans", "Standalone spans without a transaction."),
        metrics_summaries: (KafkaTopic::MetricsSummaries, "snuba-metrics-summaries", "Summary for metrics collected during a span."),
        cogs: (KafkaTopic::Cogs, "shared-resources-usage", "COGS measurements."),
        feedback: (KafkaTopic::Feedback, "ingest-feedback-events", "Feedback events topic."),
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

/// Describes Kafka config, with all the parameters extracted, which will be used for creating the
/// kafka producer.
#[derive(Debug)]
pub enum KafkaConfig<'a> {
    /// Single config with Kafka parameters.
    Single {
        /// Kafka parameters to create the kafka producer.
        params: KafkaParams<'a>,
    },
}

/// Sharded Kafka config.
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
    /// Returns [`ConfigError`] if the configuration for the current topic assignment is invalid.
    pub fn kafka_config<'a>(
        &'a self,
        default_config: &'a Vec<KafkaConfigParam>,
        secondary_configs: &'a BTreeMap<String, Vec<KafkaConfigParam>>,
    ) -> Result<KafkaConfig<'_>, ConfigError> {
        let kafka_config = match self {
            Self::Primary(topic_name) => KafkaConfig::Single {
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
                params: KafkaParams {
                    config_name: Some(kafka_config_name),
                    topic_name,
                    params: secondary_configs
                        .get(kafka_config_name)
                        .ok_or(ConfigError::UnknownKafkaConfigName)?,
                },
            },
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
        let yaml = r#"
ingest-events: "ingest-events-kafka-topic"
profiles:
    name: "ingest-profiles"
    config: "profiles"
ingest-metrics: "ingest-metrics-3"
transactions: "ingest-transactions-kafka-topic"
"#;

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

        let topics: TopicAssignments = serde_yaml::from_str(yaml).unwrap();
        let events = topics.events;
        let profiles = topics.profiles;
        let metrics_sessions = topics.metrics_sessions;
        let transactions = topics.transactions;

        assert!(matches!(events, TopicAssignment::Primary(_)));
        assert!(matches!(profiles, TopicAssignment::Secondary { .. }));
        assert!(matches!(metrics_sessions, TopicAssignment::Primary(_)));
        assert!(matches!(transactions, TopicAssignment::Primary(_)));

        let events_config = events
            .kafka_config(&def_config, &second_config)
            .expect("Kafka config for events topic");
        assert!(matches!(
            events_config,
            KafkaConfig::Single {
                params: KafkaParams {
                    topic_name: "ingest-events-kafka-topic",
                    ..
                }
            }
        ));

        let events_config = profiles
            .kafka_config(&def_config, &second_config)
            .expect("Kafka config for profiles topic");
        assert!(matches!(
            events_config,
            KafkaConfig::Single {
                params: KafkaParams {
                    topic_name: "ingest-profiles",
                    config_name: Some("profiles"),
                    ..
                }
            }
        ));

        let events_config = metrics_sessions
            .kafka_config(&def_config, &second_config)
            .expect("Kafka config for metrics topic");
        assert!(matches!(
            events_config,
            KafkaConfig::Single {
                params: KafkaParams {
                    topic_name: "ingest-metrics-3",
                    ..
                }
            }
        ));

        // Legacy keys are still supported
        let transactions_config = transactions
            .kafka_config(&def_config, &second_config)
            .expect("Kafka config for transactions topic");
        assert!(matches!(
            transactions_config,
            KafkaConfig::Single {
                params: KafkaParams {
                    topic_name: "ingest-transactions-kafka-topic",
                    ..
                }
            }
        ));
    }

    #[test]
    fn test_default_topic_is_valid() {
        let topic_assignments = TopicAssignments::default();

        // A few topics are not defined currently, remove this once added to `sentry-kafka-schemas`.
        let currrently_undefined_topics = [
            "ingest-attachments".to_string(),
            "ingest-transactions".to_string(),
            "profiles".to_string(),
            "ingest-monitors".to_string(),
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
}
