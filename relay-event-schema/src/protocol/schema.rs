use crate::protocol::Event;

/// Get the event schema as JSON schema. The return type is serde-serializable.
pub fn event_json_schema() -> impl serde::Serialize {
    schemars::schema_for!(Event)
}
