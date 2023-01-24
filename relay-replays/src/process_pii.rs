use relay_general::pii::{PiiConfig, PiiProcessor};
use relay_general::processor::{
    FieldAttrs, Pii, ProcessingState, Processor, SelectorSpec, ValueType,
};
use relay_general::types::{Meta, ProcessingAction};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;

pub type ProcessingResult = Result<Value, ParseError>;

#[derive(Debug)]
pub enum ParseError {
    InvalidPayload(&'static str),
    CouldNotScrub(ProcessingAction),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidPayload(e) => write!(f, "{e}"),
            ParseError::CouldNotScrub(e) => {
                write!(f, "could not scrub pii with error: {}", e)
            }
        }
    }
}

pub fn scrub_pii(rrweb_data: Value) -> Result<Value, ParseError> {
    let mut pii_config = PiiConfig::default();
    pii_config.applications =
        BTreeMap::from([(SelectorSpec::And(vec![]), vec!["@common".to_string()])]);

    let pii_processor = PiiProcessor::new(pii_config.compiled());
    let mut processor = RecordingProcessor::new(pii_processor);
    processor.process_events(rrweb_data)
}

struct RecordingProcessor<'a> {
    pii_processor: PiiProcessor<'a>,
}

impl RecordingProcessor<'_> {
    fn new(pii_processor: PiiProcessor) -> RecordingProcessor {
        RecordingProcessor { pii_processor }
    }

    fn process_events(&mut self, events: Value) -> ProcessingResult {
        match events {
            Value::Array(values) => Ok(Value::Array(
                values
                    .into_iter()
                    .map(|event| self.process_event(event))
                    .collect::<Result<Vec<Value>, ParseError>>()?,
            )),
            _ => Err(ParseError::InvalidPayload("expected vec of events")),
        }
    }

    fn process_event(&mut self, event: Value) -> ProcessingResult {
        match &event {
            Value::Object(obj) => match obj.get("type") {
                Some(v) => match v {
                    Value::Number(number) => match number.as_i64() {
                        Some(2) => self.process_snapshot_event(event),
                        Some(3) => self.process_incremental_snapshot_event(event),
                        Some(5) => self.process_sentry_event(event),
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

    fn process_snapshot_event(&mut self, event: Value) -> ProcessingResult {
        match event {
            Value::Object(mut obj) => match obj.get("data") {
                Some(data) => {
                    let new_data = self.process_snapshot_event_data(data.to_owned())?;
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

    fn process_snapshot_event_data(&mut self, event_data: Value) -> ProcessingResult {
        match event_data {
            Value::Object(mut obj) => {
                if let Some(node) = obj.get("node") {
                    let new_node = self.process_snapshot_node(node.to_owned())?;
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

    fn process_incremental_snapshot_event(&mut self, event: Value) -> ProcessingResult {
        match event {
            Value::Object(mut obj) => match obj.get("data") {
                Some(data) => {
                    let new_data = self.process_incremental_snapshot_event_data(data.to_owned())?;
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

    fn process_incremental_snapshot_event_data(&mut self, event: Value) -> ProcessingResult {
        match event {
            Value::Object(mut obj) => match obj.get("source") {
                Some(source) => match source {
                    Value::Number(number) => match number.as_i64() {
                        // DOM was changed.
                        Some(0) => self.process_incremental_snapshot_event_dom_mutation(&mut obj),
                        // User inputted text.
                        Some(5) => self.process_incremental_snapshot_event_text_input(&mut obj),
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
        &mut self,
        obj: &mut Map<String, Value>,
    ) -> ProcessingResult {
        if let Some(texts) = obj.get("texts") {
            obj.insert(
                "texts".to_string(),
                self.process_incremental_snapshot_event_dom_texts(texts.to_owned())?,
            );
        }

        if let Some(adds) = obj.get("adds") {
            obj.insert(
                "adds".to_string(),
                self.process_incremental_snapshot_event_dom_adds(adds.to_owned())?,
            );
        }

        Ok(Value::Object(obj.to_owned()))
    }

    fn process_incremental_snapshot_event_dom_texts(&mut self, texts: Value) -> ProcessingResult {
        match texts {
            Value::Array(text_values) => {
                let new_text_values = Value::Array(
                    text_values
                        .into_iter()
                        .map(|child| self.new_pii_scrubbed_value(&child))
                        .collect::<Result<Vec<Value>, ParseError>>()?,
                );
                Ok(new_text_values)
            }
            _ => Ok(texts),
        }
    }

    fn process_incremental_snapshot_event_dom_adds(&mut self, adds: Value) -> ProcessingResult {
        match adds {
            Value::Array(adds_values) => {
                let new_adds_values = Value::Array(
                    adds_values
                        .into_iter()
                        .map(|child| self.process_incremental_snapshot_event_dom_add(child))
                        .collect::<Result<Vec<Value>, ParseError>>()?,
                );
                Ok(new_adds_values)
            }
            _ => Ok(adds),
        }
    }

    fn process_incremental_snapshot_event_dom_add(&mut self, add: Value) -> ProcessingResult {
        match add {
            Value::Object(mut obj) => match obj.get("node") {
                Some(node) => {
                    let new_node = self.process_snapshot_node(node.to_owned())?;
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

    fn process_incremental_snapshot_event_text_input(
        &mut self,
        obj: &mut Map<String, Value>,
    ) -> ProcessingResult {
        match obj.get("text") {
            Some(text) => {
                obj.insert("text".to_string(), self.new_pii_scrubbed_value(text)?);
                Ok(Value::Object(obj.to_owned()))
            }
            None => Ok(Value::Object(obj.to_owned())),
        }
    }

    // RRWeb Node Handling.
    //
    // ...

    fn process_snapshot_node(&mut self, node: Value) -> ProcessingResult {
        match node {
            Value::Object(mut obj) => match obj.get("type") {
                Some(value) => match value {
                    Value::Number(node_type_value) => match node_type_value.as_i64() {
                        // Document node. Nothing to do here except mutate its children.
                        Some(0) => {
                            self.process_node_children(&mut obj)?;
                            Ok(Value::Object(obj))
                        }
                        // Element node. Certain attributes are sanitized and child are
                        // recursed.
                        Some(2) => {
                            self.process_element_node(&mut obj)?;
                            Ok(Value::Object(obj))
                        }
                        // Emcompasses text-nodes, CDATA nodes, and code comments.
                        Some(3) | Some(4) | Some(5) => {
                            self.process_text_node(&mut obj)?;
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
    fn process_node_children(&mut self, node: &mut Map<String, Value>) -> Result<(), ParseError> {
        // We perform mutations exclusively in this function. It does not return a new Value
        // enumeration.
        match node.get("childNodes") {
            Some(children) => match children {
                Value::Array(children) => {
                    let new_children = Value::Array(
                        children
                            .into_iter()
                            .map(|child| self.process_snapshot_node(child.to_owned()))
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

    fn process_element_node(&mut self, node: &mut Map<String, Value>) -> Result<(), ParseError> {
        match node.get("tagName") {
            Some(tag_name_value) => match tag_name_value {
                Value::String(tag_name) => match tag_name.as_str() {
                    // <style> and <script> tags are not scrubbed for PII.
                    "style" | "script" => Ok(()),
                    // <img> and <source> have their src attributes removed.
                    "img" | "source" => {
                        if let Some(attributes) = node.get("attributes") {
                            let new_attributes =
                                self.process_element_node_attributes(attributes.to_owned())?;
                            node.insert("attributes".to_string(), new_attributes);
                        }
                        self.process_node_children(node)
                    }
                    // All others recursed.
                    _ => self.process_node_children(node),
                },
                _ => Err(ParseError::InvalidPayload(
                    "element node tagName key must have value string",
                )),
            },
            None => Err(ParseError::InvalidPayload("element node missing tagName")),
        }
    }

    fn process_element_node_attributes(&mut self, node: Value) -> ProcessingResult {
        match node {
            Value::Object(mut attributes) => {
                if attributes.contains_key("src") {
                    attributes.insert("src".to_string(), Value::String("#".to_string()));
                }
                Ok(Value::Object(attributes))
            }
            _ => Err(ParseError::InvalidPayload(
                "expected attributes object on element node to be of type object",
            )),
        }
    }

    /// Text Node processor.
    ///
    /// We expect text-nodes to contain a key "textContent" whose value is of type string.
    ///
    /// { "textContent": "lorem lipsum" }
    fn process_text_node(&mut self, node: &mut Map<String, Value>) -> Result<(), ParseError> {
        if let Some(text_context) = node.get("textContent") {
            node.insert(
                "textContent".to_string(),
                self.new_pii_scrubbed_value(text_context)?,
            );
        }
        Ok(())
    }

    // Sentry Event Handling.
    //
    // ...

    fn process_sentry_event(&mut self, event: Value) -> ProcessingResult {
        match event {
            Value::Object(mut wrapper) => match wrapper.get("data") {
                Some(data) => {
                    wrapper.insert(
                        "data".to_string(),
                        self.process_sentry_event_data(data.to_owned())?,
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

    fn process_sentry_event_data(&mut self, value: Value) -> ProcessingResult {
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
                        "performanceSpan" => self.process_performance_span(event_payload),
                        "breadcrumb" => self.process_breadcrumb(event_payload),
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

    fn process_performance_span(&mut self, value: Value) -> ProcessingResult {
        if let Value::Object(mut obj) = value {
            match obj.get("description") {
                Some(description) => {
                    obj.insert(
                        "description".to_string(),
                        self.new_pii_scrubbed_value(description)?,
                    );
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

    fn process_breadcrumb(&mut self, value: Value) -> ProcessingResult {
        if let Value::Object(mut obj) = value {
            match obj.get("message") {
                Some(message) => {
                    obj.insert("message".to_string(), self.new_pii_scrubbed_value(message)?);
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

    fn new_pii_scrubbed_value(&mut self, value: &Value) -> ProcessingResult {
        match value {
            Value::String(s) => match self.remove_pii_from_string(s) {
                Ok(scrubbed_string) => Ok(Value::String(scrubbed_string)),
                Err(e) => Err(ParseError::CouldNotScrub(e)),
            },
            _ => Err(ParseError::InvalidPayload("expected string value")),
        }
    }

    fn remove_pii_from_string(&mut self, string: &String) -> Result<String, ProcessingAction> {
        let field_attrs = Cow::Owned(FieldAttrs::new().pii(Pii::True));
        let processing_state =
            ProcessingState::root().enter_static("", Some(field_attrs), Some(ValueType::String));

        let mut new_string = string.clone();

        self.pii_processor.process_string(
            &mut new_string,
            &mut Meta::default(),
            &processing_state,
        )?;

        Ok(new_string)
    }
}

#[cfg(test)]
mod tests {
    use crate::process_pii::scrub_pii;
    use serde_json::Value;
    use std::str;

    #[test]
    fn test_scrub_custom_event() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-5.json");
        let payload: Value = serde_json::from_slice(payload).unwrap();

        let scrubbed_payload = scrub_pii(payload).unwrap();

        let scrubbed_result = serde_json::to_vec(&scrubbed_payload).unwrap();
        let stringy_json = str::from_utf8(&scrubbed_result).unwrap();
        assert!(stringy_json.contains("\"description\":\"[creditcard]\""));
        assert!(stringy_json.contains("\"description\":\"https://sentry.io?ip-address=[ip]\""));
        assert!(stringy_json.contains("\"message\":\"[email]\""));
    }

    #[test]
    fn test_scrub_incremental_snapshot_event() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-3.json");
        let payload: Value = serde_json::from_slice(payload).unwrap();

        let scrubbed_payload = scrub_pii(payload).unwrap();

        let scrubbed_result = serde_json::to_vec(&scrubbed_payload).unwrap();
        let stringy_json = str::from_utf8(&scrubbed_result).unwrap();
        assert!(stringy_json.contains("\"textContent\":\"[creditcard]\""));
    }

    #[test]
    fn test_scrub_full_snapshot_event() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-2.json");
        let payload: Value = serde_json::from_slice(payload).unwrap();

        let scrubbed_payload = scrub_pii(payload).unwrap();

        let scrubbed_result = serde_json::to_vec(&scrubbed_payload).unwrap();
        let stringy_json = str::from_utf8(&scrubbed_result).unwrap();
        assert!(stringy_json.contains("{\"attributes\":{\"src\":\"#\"}"));
        assert!(stringy_json.contains("\"textContent\":\"my ssn is ***********\""));
    }
}
