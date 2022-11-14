//! Kafka-related functionality.
//!
//! By default, this library only provides the support for `relay` configuration. With `producer`
//! feature enabled, the [`KafkaClient`] is provided and can be used to send messages to the Kafka
//! broker.
//!
//! # Usage
//!
//! ```no_run
//!     use relay_kafka::{KafkaClient, KafkaTopic};
//!
//!     # let params = relay_kafka::KafkaParams{
//!     #     topic_name: "test",
//!     #     config_name: None,
//!     #     params: &[relay_kafka::KafkaConfigParam{name: "test".into(), value: "test".into()}]
//!     # };
//!     # let kafka_config_events = relay_kafka::KafkaConfig::Single{ params };
//!
//!     # struct M {}
//!     # impl relay_kafka::Message for M {
//!     #   fn key(&self) -> [u8; 16] { todo!() }
//!     #   fn variant(&self) -> &'static str { "test" }
//!     #   fn serialize(&self) -> Result<Vec<u8>, relay_kafka::ClientError> { todo!() }
//!     # }
//!     #
//!     # let kafka_message = M{};
//!
//!     // Get the builder.
//!     let mut builder = KafkaClient::builder();
//!
//!     // Then one can add different configs per topic.
//!     builder = builder.add_kafka_topic_config(KafkaTopic::Events, &kafka_config_events).unwrap();
//!
//!     // And add potentially another topic config.
//!     // builder.add_kafka_topic_config(&kafka_config_metrics)?;
//!
//!     // build the client
//!     let kafka_client = builder.build();
//!     
//!     // send the message
//!     kafka_client.send_message(KafkaTopic::Events, 1u64, &kafka_message).unwrap();
//! ```
//!
//! If the configuration for the [`KafkaTopic`] was not added, attemps to send the message to this
//! topic will return the error.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod config;
#[cfg(feature = "producer")]
mod producer;
#[cfg(feature = "producer")]
mod statsd;

pub use config::*;
#[cfg(feature = "producer")]
pub use producer::*;
