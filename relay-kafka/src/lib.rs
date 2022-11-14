//! Kafka-related functionality.
//!
//! By default, this library only provides the support for `relay` configuration. With `producer`
//! feature enabled, the [`KafkaClient`] is provided and can be used to send messages to the Kafka
//! broker.
//!
//! # Usage
//!
//! ```no_run
//!
//!     let kafka_config_events = ...;
//!     let kafka_config_metrics = ...;
//!     let mut builder = KafkaClient::builder();
//!     builder.add_kafka_topic_config(&kafka_config_events)?;
//!     builder.add_kafka_topic_config(&kafka_config_metrics)?;
//!
//!     let kafka_client = builder.build();
//!
//!     let kafka_message = ...;
//!     kafka_client.send_message(KafkaTopic::Events, &kafka_message)?;
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
