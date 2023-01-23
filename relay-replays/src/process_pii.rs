use serde_json::{Error, Map, Value};
use std::fmt::Display;

pub type ProcessingResult = Result<Value, ParseError>;

#[derive(Debug)]
pub enum ParseError {
    InvalidPayload(&'static str),
    CouldNotSerialize(Error),
    CouldNotDeserialize(Error),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidPayload(e) => write!(f, "{e}"),
            ParseError::CouldNotSerialize(e) => {
                write!(f, "could not serialize scrubbed output with error: {}", e)
            }
            ParseError::CouldNotDeserialize(e) => {
                write!(f, "could not deserialize input data with error: {}", e)
            }
        }
    }
}

pub fn scrub_pii(rrweb_data: &[u8]) -> Result<Vec<u8>, ParseError> {
    let result: Result<Value, Error> = serde_json::from_slice(rrweb_data);

    match result {
        Ok(events) => {
            let scrubbed_events = process_events(events)?;
            match serde_json::to_vec(&scrubbed_events) {
                Ok(scrubbed_bytes) => Ok(scrubbed_bytes),
                Err(e) => Err(ParseError::CouldNotSerialize(e)),
            }
        }
        Err(e) => Err(ParseError::CouldNotDeserialize(e)),
    }
}

fn process_events(events: Value) -> ProcessingResult {
    match events {
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(|event| process_event(event))
                .collect::<Result<Vec<Value>, ParseError>>()?,
        )),
        _ => Err(ParseError::InvalidPayload("expected vec of events")),
    }
}

fn process_event(event: Value) -> ProcessingResult {
    match &event {
        Value::Object(obj) => match obj.get("type") {
            Some(v) => match v {
                Value::Number(number) => match number.as_i64() {
                    Some(2) => process_snapshot_event(event),
                    Some(3) => process_incremental_snapshot_event(event),
                    Some(5) => process_sentry_event(event),
                    _ => Ok(v.to_owned()),
                },
                _ => Err(ParseError::InvalidPayload("event type must be a number")),
            },
            None => Err(ParseError::InvalidPayload(
                "missing required type field for event",
            )),
        },
        _ => Err(ParseError::InvalidPayload("expected object type")),
    }
}

// RRWeb Snapshot Event Handling.
//
// ...

fn process_snapshot_event(event: Value) -> ProcessingResult {
    match event {
        Value::Object(mut obj) => match obj.get("data") {
            Some(data) => {
                let new_data = process_snapshot_event_data(data.to_owned())?;
                obj.insert("data".to_string(), new_data);
                Ok(Value::Object(obj))
            }
            None => Err(ParseError::InvalidPayload(
                "expected snapshot event to contain data key",
            )),
        },
        _ => Err(ParseError::InvalidPayload(
            "expected snapshot event to be of type object",
        )),
    }
}

fn process_snapshot_event_data(event_data: Value) -> ProcessingResult {
    match event_data {
        Value::Object(mut obj) => {
            if let Some(node) = obj.get("node") {
                let new_node = process_snapshot_node(node.to_owned())?;
                obj.insert("node".to_string(), new_node);
            }
            Ok(Value::Object(obj))
        }
        _ => Err(ParseError::InvalidPayload(
            "expected snapshot event data key to be of type object",
        )),
    }
}

// RRWeb Incremental Event Handling.
//
// ...

fn process_incremental_snapshot_event(event: Value) -> ProcessingResult {
    match event {
        Value::Object(mut obj) => match obj.get("data") {
            Some(data) => {
                let new_data = process_incremental_snapshot_event_data(data.to_owned())?;
                obj.insert("data".to_string(), new_data);
                Ok(Value::Object(obj))
            }
            None => Ok(Value::Object(obj)),
        },
        _ => Err(ParseError::InvalidPayload(
            "expected incremental snapshot event to be of type object",
        )),
    }
}

fn process_incremental_snapshot_event_data(event: Value) -> ProcessingResult {
    match event {
        Value::Object(mut obj) => match obj.get("source") {
            Some(source) => match source {
                Value::Number(number) => match number.as_i64() {
                    // DOM was changed.
                    Some(0) => process_incremental_snapshot_event_dom_mutation(&mut obj),
                    // User inputted text.
                    Some(5) => process_incremental_snapshot_event_text_input(&mut obj),
                    // Unhandled source types are preserved.
                    _ => Ok(Value::Object(obj)),
                },
                _ => Err(ParseError::InvalidPayload(
                    "expected incremental snapshot event source field to be of type number",
                )),
            },
            None => Err(ParseError::InvalidPayload(
                "expected incremental snapshot event to contain source field",
            )),
        },
        _ => Err(ParseError::InvalidPayload(
            "expected incremental snapshot event data key to be of type object",
        )),
    }
}

fn process_incremental_snapshot_event_dom_mutation(
    obj: &mut Map<String, Value>,
) -> ProcessingResult {
    if let Some(texts) = obj.get("texts") {
        obj.insert(
            "texts".to_string(),
            process_incremental_snapshot_event_dom_texts(texts.to_owned())?,
        );
    }

    if let Some(adds) = obj.get("adds") {
        obj.insert(
            "adds".to_string(),
            process_incremental_snapshot_event_dom_adds(adds.to_owned())?,
        );
    }

    Ok(Value::Object(obj.to_owned()))
}

fn process_incremental_snapshot_event_dom_texts(texts: Value) -> ProcessingResult {
    match texts {
        Value::Array(text_values) => {
            let new_text_values = Value::Array(
                text_values
                    .into_iter()
                    .map(|child| process_value(&child))
                    .collect::<Result<Vec<Value>, ParseError>>()?,
            );
            Ok(new_text_values)
        }
        _ => Ok(texts),
    }
}

fn process_incremental_snapshot_event_dom_adds(adds: Value) -> ProcessingResult {
    match adds {
        Value::Array(adds_values) => {
            let new_adds_values = Value::Array(
                adds_values
                    .into_iter()
                    .map(|child| process_incremental_snapshot_event_dom_add(child))
                    .collect::<Result<Vec<Value>, ParseError>>()?,
            );
            Ok(new_adds_values)
        }
        _ => Ok(adds),
    }
}

fn process_incremental_snapshot_event_dom_add(add: Value) -> ProcessingResult {
    match add {
        Value::Object(mut obj) => match obj.get("node") {
            Some(node) => {
                let new_node = process_snapshot_node(node.to_owned())?;
                obj.insert("node".to_string(), new_node);
                Ok(Value::Object(obj))
            }
            None => Ok(Value::Object(obj)),
        },
        _ => Err(ParseError::InvalidPayload(
            "expected incremental addition to be of type object",
        )),
    }
}

fn process_incremental_snapshot_event_text_input(obj: &mut Map<String, Value>) -> ProcessingResult {
    match obj.get("text") {
        Some(text) => {
            obj.insert("text".to_string(), process_value(text)?);
            Ok(Value::Object(obj.to_owned()))
        }
        None => Ok(Value::Object(obj.to_owned())),
    }
}

// RRWeb Node Handling.
//
// ...

fn process_snapshot_node(node: Value) -> ProcessingResult {
    match node {
        Value::Object(mut obj) => match obj.get("type") {
            Some(value) => match value {
                Value::Number(node_type_value) => match node_type_value.as_i64() {
                    // Document node. Nothing to do here except mutate its children.
                    Some(0) => {
                        process_node_children(&mut obj)?;
                        Ok(Value::Object(obj))
                    }
                    // Element node. Certain attributes are sanitized and child are
                    // recursed.
                    Some(2) => {
                        process_element_node(&mut obj)?;
                        Ok(Value::Object(obj))
                    }
                    // Emcompasses text-nodes, CDATA nodes, and code comments.
                    Some(3) | Some(4) | Some(5) => {
                        process_text_node(&mut obj)?;
                        Ok(Value::Object(obj))
                    }
                    // Unhandled types return themselves and are not processed in any way.
                    _ => Ok(Value::Object(obj)),
                },
                _ => Err(ParseError::InvalidPayload(
                    "expected snapshot node type to be a number",
                )),
            },
            None => Err(ParseError::InvalidPayload(
                "expected object type value in snapshot node",
            )),
        },
        _ => Err(ParseError::InvalidPayload(
            "expected object type in snapshot node",
        )),
    }
}

/// Node children processor.
///
/// We expect nodes which can have children to contain a key called "childNodes" whose value is
/// Array(Vec<Node>).
///
/// { "childNodes": [..., ...] }
fn process_node_children(node: &mut Map<String, Value>) -> Result<(), ParseError> {
    // We perform mutations exclusively in this function. It does not return a new Value
    // enumeration.
    match node.get("childNodes") {
        Some(children) => match children {
            Value::Array(children) => {
                let new_children = Value::Array(
                    children
                        .into_iter()
                        .map(|child| process_snapshot_node(child.to_owned()))
                        .collect::<Result<Vec<Value>, ParseError>>()?,
                );
                node.insert("childNodes".to_string(), new_children);
                Ok(())
            }
            // Sensitive area of the schema. We do not trust provider.
            _ => Err(ParseError::InvalidPayload(
                "expected an array of children in snapshot node",
            )),
        },
        // We don't have to error here. We could trust the provider. But this is an important
        // bit of the schema and we can't risk missing PII.  It's safer to err and debug these
        // malformed payloads than potentially accept PII.
        None => Err(ParseError::InvalidPayload("missing node children")),
    }
}

/// Element Node processor.

fn process_element_node(node: &mut Map<String, Value>) -> Result<(), ParseError> {
    match node.get("tagName") {
        Some(tag_name_value) => match tag_name_value {
            Value::String(tag_name) => match tag_name.as_str() {
                // <style> and <script> tags are not scrubbed for PII.
                "style" | "script" => Ok(()),
                // <img> and <source> have their src attributes removed.
                "img" | "source" => {
                    if let Some(attributes) = node.get("attributes") {
                        process_element_node_attributes(attributes.to_owned());
                    }
                    process_node_children(node)
                }
                // All others recursed.
                _ => process_node_children(node),
            },
            _ => Err(ParseError::InvalidPayload(
                "element node tagName key must have value string",
            )),
        },
        None => Err(ParseError::InvalidPayload("element node missing tagName")),
    }
}

fn process_element_node_attributes(node: Value) {
    match node {
        Value::Object(mut attributes) => {
            if attributes.contains_key("src") {
                attributes.insert("src".to_string(), Value::String("#".to_string()));
            }
        }
        _ => {}
    }
}

/// Text Node processor.
///
/// We expect text-nodes to contain a key "textContent" whose value is of type string.
///
/// { "textContent": "lorem lipsum" }
fn process_text_node(node: &mut Map<String, Value>) -> Result<(), ParseError> {
    if let Some(text_context) = node.get("textContent") {
        node.insert("textContent".to_string(), process_value(text_context)?);
    }
    Ok(())
}

// Sentry Event Handling.
//
// ...

fn process_sentry_event(event: Value) -> ProcessingResult {
    match event {
        Value::Object(mut wrapper) => match wrapper.get("data") {
            Some(data) => {
                wrapper.insert(
                    "data".to_string(),
                    process_sentry_event_data(data.to_owned())?,
                );
                Ok(Value::Object(wrapper))
            }
            None => Err(ParseError::InvalidPayload(
                "missing data key for sentry event",
            )),
        },
        _ => Err(ParseError::InvalidPayload("expected object type")),
    }
}

fn process_sentry_event_data(value: Value) -> ProcessingResult {
    match value {
        Value::Object(mut data) => {
            let event_type = data
                .get("tag")
                .ok_or(ParseError::InvalidPayload("missing tag key"))?;
            let event_payload = data
                .get("payload")
                .ok_or(ParseError::InvalidPayload("missing payload key"))?
                .to_owned();

            if let Value::String(s) = event_type {
                let payload_value = match s.as_str() {
                    "performanceSpan" => process_performance_span(event_payload),
                    "breadcrumb" => process_breadcrumb(event_payload),
                    _ => Err(ParseError::InvalidPayload("test")),
                }?;

                data.insert(String::from("payload"), payload_value);
                Ok(Value::Object(data))
            } else {
                Err(ParseError::InvalidPayload("expected string"))
            }
        }
        _ => Err(ParseError::InvalidPayload(
            "expected object type for data key in sentry event",
        )),
    }
}

fn process_performance_span(value: Value) -> ProcessingResult {
    if let Value::Object(mut obj) = value {
        match obj.get("description") {
            Some(description) => {
                obj.insert("description".to_string(), process_value(description)?);
                Ok(Value::Object(obj))
            }
            None => Ok(Value::Object(obj)),
        }
    } else {
        Err(ParseError::InvalidPayload(
            "expected performanceSpan payload to be an object",
        ))
    }
}

fn process_breadcrumb(value: Value) -> ProcessingResult {
    if let Value::Object(mut obj) = value {
        match obj.get("message") {
            Some(message) => {
                obj.insert("message".to_string(), process_value(message)?);
                Ok(Value::Object(obj))
            }
            None => Ok(Value::Object(obj)),
        }
    } else {
        Err(ParseError::InvalidPayload(
            "expected breadcrumb payload to be an object",
        ))
    }
}

fn process_value(value: &Value) -> ProcessingResult {
    match value {
        Value::String(s) => Ok(Value::String("*****".to_string())),
        _ => Err(ParseError::InvalidPayload("expected string value")),
    }
}

#[cfg(test)]
mod tests {
    use crate::process_pii::scrub_pii;
    use serde_json::Value;

    #[test]
    fn test() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-5.json");
        let x = scrub_pii(payload);

        // let x: Value = serde_json::from_slice(&x).unwrap();
        // println!("{:?}", x);
        // assert!(false);
    }

    #[test]
    fn test_snapshot() {
        let payload = include_bytes!("../tests/fixtures/rrweb.json");
        let x = scrub_pii(payload);
    }

    #[test]
    fn test_diff() {
        let payload = include_bytes!("../tests/fixtures/rrweb-pii.json");
        let x = scrub_pii(payload).unwrap();

        let x: Value = serde_json::from_slice(&x).unwrap();
        println!("{:?}", x);
        assert!(false);
    }
}
