use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
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
    LogicalTopic,

    /// Failed to deserialize message, potentially because it isn't JSON?
    #[error("failed to deserialize message")]
    MessageJson(#[source] serde_json::Error),

    /// Failed to deserialize schema as JSON
    #[error("failed to deserialize schema")]
    SchemaJson(#[source] serde_json::Error),

    /// Failed to compile schema
    // We stringify the inner error because `jsonschema::ValidationError` has weird lifetimes
    #[error("failed to compile schema: {0}")]
    SchemaCompiled(String),

    /// Failed to validate message JSON against schema
    // We stringify the inner error because `jsonschema::ValidationError` has weird lifetimes
    #[error("message violates schema: {0}")]
    Message(String),
}

/// Validates payloads for their given topic's schema.
#[derive(Debug, Default)]
pub struct Validator {
    /// Caches the schema for given topics.
    schemas: BTreeMap<KafkaTopic, Option<JSONSchema>>,
}

impl Validator {
    /// Validate a message for a given topic's schema.
    pub fn validate_message_schema(
        &mut self,
        topic: KafkaTopic,
        message: &[u8],
    ) -> Result<(), SchemaError> {
        let Some(schema) = self.get_schema(topic)? else {
            return Ok(());
        };
        let message_value = serde_json::from_slice(message).map_err(SchemaError::MessageJson)?;

        if let Err(e) = schema.validate(&message_value) {
            let mut result = String::new();
            for error in e {
                writeln!(result, "{error}").unwrap();
            }

            return Err(SchemaError::Message(result));
        }

        Ok(())
    }

    fn get_schema(&mut self, topic: KafkaTopic) -> Result<Option<&JSONSchema>, SchemaError> {
        Ok(match self.schemas.entry(topic) {
            Entry::Vacant(entry) => {
                entry.insert({
                    let default_assignments = TopicAssignments::default();
                    let logical_topic_name = match default_assignments.get(topic) {
                        TopicAssignment::Primary(logical_topic_name) => logical_topic_name,
                        _ => return Err(SchemaError::LogicalTopic),
                    };

                    let schema = match sentry_kafka_schemas::get_schema(logical_topic_name, None) {
                        Ok(schema) => schema,
                        // No topic found
                        Err(_) => return Ok(None),
                    };
                    let schema =
                        serde_json::from_str(&schema.schema).map_err(SchemaError::SchemaJson)?;
                    let schema = JSONSchema::compile(&schema)
                        .map_err(|e| SchemaError::SchemaCompiled(e.to_string()))?;
                    Some(schema)
                })
            }
            Entry::Occupied(entry) => entry.into_mut(),
        }
        .as_ref())
    }
}
