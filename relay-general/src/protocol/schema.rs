use crate::protocol::Event;

pub fn event_json_schema() -> impl serde::Serialize {
    schemars::schema_for!(Event)
}
