use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use sentry_kafka_schemas::{Schema as SentrySchema, SchemaError as SentrySchemaError};
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

    /// Failed to compile schema
    // We stringify the inner error because `jsonschema::ValidationError` has weird lifetimes
    #[error("failed to compile schema: {0}")]
    SchemaCompiled(SentrySchemaError),

    /// Failed to validate message JSON against schema
    // We stringify the inner error because `jsonschema::ValidationError` has weird lifetimes
    #[error("message violates schema: {0}")]
    Validation(SentrySchemaError),
}

/// Validates payloads for their given topic's schema.
#[derive(Debug, Default)]
pub struct Validator {
    /// Caches the schema for given topics.
    schemas: Mutex<BTreeMap<KafkaTopic, Option<Arc<SentrySchema>>>>,
}

impl Validator {
    /// Validate a message for a given topic's schema.
    pub fn validate_message_schema(
        &self,
        topic: KafkaTopic,
        message: &[u8],
    ) -> Result<(), SchemaError> {
        let Some(schema) = self.get_schema(topic)? else {
            return Ok(());
        };

        schema
            .validate_json(message)
            .map_err(SchemaError::Validation)
            .map(drop)
    }

    fn get_schema(&self, topic: KafkaTopic) -> Result<Option<Arc<SentrySchema>>, SchemaError> {
        let mut schemas = self.schemas.lock().unwrap_or_else(|e| e.into_inner());

        Ok(match schemas.entry(topic) {
            Entry::Vacant(entry) => entry.insert({
                let default_assignments = TopicAssignments::default();
                let logical_topic_name = match default_assignments.get(topic) {
                    TopicAssignment::Primary(logical_topic_name) => logical_topic_name,
                    _ => return Err(SchemaError::LogicalTopic),
                };

                match sentry_kafka_schemas::get_schema(logical_topic_name, None) {
                    Ok(schema) => Some(Arc::new(schema)),
                    Err(SentrySchemaError::TopicNotFound) => None,
                    Err(err) => return Err(SchemaError::SchemaCompiled(err)),
                }
            }),
            Entry::Occupied(entry) => entry.into_mut(),
        }
        .as_ref()
        .map(Arc::clone))
    }
}
