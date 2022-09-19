pub mod config;
#[cfg(feature = "processing")]
mod producer;
#[cfg(feature = "processing")]
mod utils;

use failure::Fail;

#[cfg(feature = "processing")]
pub use producer::*;

/// Kafka producer errors.
#[cfg(feature = "processing")]
#[derive(Fail, Debug)]
pub enum ProducerError {
    #[fail(display = "failed to send kafka message")]
    SendFailed(#[cause] rdkafka::error::KafkaError),
    #[fail(display = "failed to create kafka producer: invalid kafka config")]
    InvalidConfig(#[cause] rdkafka::error::KafkaError),
    #[fail(display = "failed to serialize kafka message")]
    InvalidMsgPack(#[cause] rmp_serde::encode::Error),
    #[fail(display = "failed to serialize json message")]
    InvalidJson(#[cause] serde_json::Error),
    #[fail(display = "invalid kafka shard")]
    InvalidShard,
}

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

/// Describes the type which can be sent using kafka producer provided by this crate.
#[cfg(feature = "processing")]
pub trait Message {
    /// Returns the partitioning key for this kafka message determining.
    fn key(&self) -> [u8; 16];

    /// Returns the type of the message.
    fn variant(&self) -> &'static str;

    /// Serializes the message into its binary format.
    ///
    /// # Errors
    /// Returns the [`ProducerError::InvalidMsgPack`] or [`ProducerError::InvalidJson`] if the
    /// serialization failed.
    fn serialize(&self) -> Result<Vec<u8>, ProducerError>;
}
