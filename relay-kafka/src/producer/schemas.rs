use std::fmt::Write;

use jsonschema::JSONSchema;
use thiserror::Error;

use crate::config::{KafkaTopic, TopicAssignment, TopicAssignments};

#[derive(Debug, Error)]
pub enum SchemaError {
    /// The "logical topic" is a concept in sentry-kafka-schemas and Snuba that identifies topics
    /// irrespective of how they are structured in production. Snuba has a mapping from "logical
    /// topic" to "physical topic" that is very similar to our `TopicAssignments`.
    ///
    /// The name of a logical topic is almost always equal to the Kafka topic's name in local
    /// development. So, in order to determine a logical topic name from a `KafkaTopic`, we get
    /// `TopicAssignments::default()` and try to resolve the name through that.
    ///
    /// When doing that we assume that Relay's default topic assignments don't use slicing/sharding
    /// and no custom clusters.
    ///
    /// If somebody changes `impl Default for TopicAssignments` to have more complex defaults, this
    /// error will start occurring. But it should not happen in prod.
    #[error("failed to determine logical topic")]
    InvalidLogicalTopic,

    /// Failed to deserialize message, potentially because it isn't JSON?
    #[error("failed to deserialize message")]
    InvalidMessageJson(#[source] serde_json::Error),

    /// Failed to deserialize schema as JSON
    #[error("failed to deserialize schema")]
    InvalidSchemaJson(#[source] serde_json::Error),

    /// Failed to compile schema
    // We stringify the inner error because `jsonschema::ValidationError` has weird lifetimes
    #[error("failed to compile schema: {0}")]
    InvalidSchemaCompiled(String),

    /// Failed to validate message JSON against schema
    // We stringify the inner error because `jsonschema::ValidationError` has weird lifetimes
    #[error("message violates schema: {0}")]
    InvalidMessage(String),
}

pub fn validate_message_schema(topic: KafkaTopic, message: &[u8]) -> Result<(), SchemaError> {
    let default_assignments = TopicAssignments::default();
    let logical_topic_name = match default_assignments.get(topic) {
        TopicAssignment::Primary(logical_topic_name) => logical_topic_name,
        _ => return Err(SchemaError::InvalidLogicalTopic),
    };

    let schema = match sentry_kafka_schemas::get_schema(logical_topic_name, None) {
        Ok(schema) => schema,
        // No topic found
        Err(_) => return Ok(()),
    };
    let schema = serde_json::from_str(&schema.schema).map_err(SchemaError::InvalidSchemaJson)?;
    let schema = JSONSchema::compile(&schema)
        .map_err(|e| SchemaError::InvalidSchemaCompiled(e.to_string()))?;
    let message_value =
        serde_json::from_slice(&message).map_err(SchemaError::InvalidMessageJson)?;

    if let Err(e) = schema.validate(&message_value) {
        let mut result = String::new();
        for error in e {
            writeln!(result, "{}", error).unwrap();
        }

        return Err(SchemaError::InvalidMessage(result));
    }

    Ok(())
}
