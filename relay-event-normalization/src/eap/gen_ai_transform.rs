//! Transforms deprecated gen_ai attributes into their normalized replacements.
//!
//! These transformations go beyond simple renaming — they reshape attribute values
//! to match the new schema. For the transformation specifications, see:
//! <https://github.com/getsentry/sentry-conventions/tree/main/model/attribute_transformations>
//!
//! Attributes with `_status: "transform"` in sentry-conventions produce
//! `WriteBehavior::CurrentName`, so [`super::normalize_attribute_names`] leaves
//! them alone — the full move-and-reshape is handled here.
//!
//! The functions are generic over [`AttributesLike`] so they work on both
//! [`Attributes`](relay_event_schema::protocol::Attributes) (SpanV2) and
//! [`SpanData`](relay_event_schema::protocol::SpanData) (SpanV1).

// This module intentionally reads deprecated attribute keys to transform their values.
#![allow(deprecated)]

use relay_conventions::attributes::*;
use serde::{Deserialize, Serialize};

use super::attribute_like::{AttributeLike, AttributesLike};

// --- Input models (what SDKs send) ---

/// A message in the old `gen_ai.request.messages` format.
#[derive(Deserialize)]
struct OldMessage {
    #[serde(default)]
    content: Option<OldContent>,
    #[serde(flatten)]
    rest: serde_json::Map<String, serde_json::Value>,
}

/// The `content` field of an old message — either a string or an array of parts.
#[derive(Deserialize)]
#[serde(untagged)]
enum OldContent {
    String(String),
    Parts(Vec<serde_json::Map<String, serde_json::Value>>),
}

/// The `gen_ai.response.text` attribute — can be various shapes.
#[derive(Deserialize)]
#[serde(untagged)]
enum ResponseText {
    /// A plain JSON string.
    String(String),
    /// A JSON array (of strings or objects).
    Array(Vec<ResponseTextItem>),
    /// A JSON object with a `content` field.
    Object { content: serde_json::Value },
}

/// An item in a `gen_ai.response.text` array.
#[derive(Deserialize)]
#[serde(untagged)]
enum ResponseTextItem {
    String(String),
    Object { content: serde_json::Value },
}

// --- Output models (what we produce) ---

/// A message in the new `gen_ai.input.messages` / `gen_ai.output.messages` format.
#[derive(Serialize)]
struct NewMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<serde_json::Value>,
    #[serde(flatten)]
    rest: serde_json::Map<String, serde_json::Value>,
}

/// A tool call entry from `gen_ai.response.tool_calls`.
#[derive(Deserialize)]
struct ToolCall {
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}

// --- Transformation logic ---

/// Applies gen_ai attribute transformations.
pub(crate) fn transform_gen_ai<T: AttributesLike>(attributes: &mut T) {
    transform_request_messages(attributes);
    transform_response_to_output_messages(attributes);
}

/// Transforms `gen_ai.request.messages` → `gen_ai.input.messages`.
fn transform_request_messages<T: AttributesLike>(attributes: &mut T) {
    if attributes.contains_key(GEN_AI__INPUT__MESSAGES) {
        attributes.remove(GEN_AI__REQUEST__MESSAGES);
        return;
    }

    let Some(raw) = get_str(attributes, GEN_AI__REQUEST__MESSAGES) else {
        return;
    };

    let Ok(messages) = serde_json::from_str::<Vec<OldMessage>>(&raw) else {
        // Not a valid message array — move as-is (just a key rename).
        if let Some(attr) = attributes.remove(GEN_AI__REQUEST__MESSAGES) {
            attributes.insert(GEN_AI__INPUT__MESSAGES.to_owned(), attr);
        }
        return;
    };

    let new_messages: Vec<NewMessage> = messages
        .into_iter()
        .map(|msg| {
            let OldMessage { content, mut rest } = msg;
            let role = rest
                .remove("role")
                .and_then(|v| v.as_str().map(String::from));

            let parts = match content {
                Some(OldContent::String(s)) => {
                    vec![serde_json::json!({"type": "text", "content": s})]
                }
                Some(OldContent::Parts(items)) => items
                    .into_iter()
                    .map(|mut part| {
                        if !part.contains_key("content") {
                            if let Some(text) = part.get("text").cloned() {
                                part.insert("content".to_owned(), text);
                            }
                        }
                        serde_json::Value::Object(part)
                    })
                    .collect(),
                None => {
                    // No content field — put everything back and skip.
                    if let Some(role) = role {
                        rest.insert("role".to_owned(), serde_json::Value::String(role));
                    }
                    // Return the original object unchanged since there's nothing to transform.
                    // We still need to return a NewMessage, so we use an empty parts vec
                    // and let the rest carry the original fields.
                    return NewMessage {
                        role: None,
                        parts: vec![],
                        rest,
                    };
                }
            };

            NewMessage { role, parts, rest }
        })
        .collect();

    if let Ok(json) = serde_json::to_string(&new_messages) {
        attributes.insert(
            GEN_AI__INPUT__MESSAGES.to_owned(),
            T::Value::from(json).into(),
        );
    }
    attributes.remove(GEN_AI__REQUEST__MESSAGES);
}

/// Transforms `gen_ai.response.text` + `gen_ai.response.tool_calls` → `gen_ai.output.messages`.
fn transform_response_to_output_messages<T: AttributesLike>(attributes: &mut T) {
    if attributes.contains_key(GEN_AI__OUTPUT__MESSAGES) {
        attributes.remove(GEN_AI__RESPONSE__TEXT);
        attributes.remove(GEN_AI__RESPONSE__TOOL_CALLS);
        return;
    }

    let response_text = get_str(attributes, GEN_AI__RESPONSE__TEXT);
    let tool_calls_raw = get_str(attributes, GEN_AI__RESPONSE__TOOL_CALLS);

    if response_text.is_none() && tool_calls_raw.is_none() {
        return;
    }

    let mut parts = Vec::new();

    if let Some(ref text) = response_text {
        extract_text_parts(text, &mut parts);
    }

    if let Some(ref raw) = tool_calls_raw {
        extract_tool_call_parts(raw, &mut parts);
    }

    // Always clean up deprecated keys.
    attributes.remove(GEN_AI__RESPONSE__TEXT);
    attributes.remove(GEN_AI__RESPONSE__TOOL_CALLS);

    if parts.is_empty() {
        return;
    }

    let output = [NewMessage {
        role: Some("assistant".to_owned()),
        parts,
        rest: Default::default(),
    }];
    if let Ok(json) = serde_json::to_string(&output) {
        attributes.insert(
            GEN_AI__OUTPUT__MESSAGES.to_owned(),
            T::Value::from(json).into(),
        );
    }
}

/// Extracts text parts from a `gen_ai.response.text` value.
fn extract_text_parts(raw: &str, parts: &mut Vec<serde_json::Value>) {
    let Ok(parsed) = serde_json::from_str::<ResponseText>(raw) else {
        // Not valid JSON — treat the entire raw string as plain text.
        parts.push(serde_json::json!({"type": "text", "content": raw}));
        return;
    };

    match parsed {
        ResponseText::String(s) => {
            parts.push(serde_json::json!({"type": "text", "content": s}));
        }
        ResponseText::Array(items) => {
            for item in items {
                match item {
                    ResponseTextItem::String(s) => {
                        parts.push(serde_json::json!({"type": "text", "content": s}));
                    }
                    ResponseTextItem::Object { content } => {
                        parts.push(serde_json::json!({"type": "text", "content": content}));
                    }
                }
            }
        }
        ResponseText::Object { content } => {
            parts.push(serde_json::json!({"type": "text", "content": content}));
        }
    }
}

/// Extracts tool_call parts from a `gen_ai.response.tool_calls` value.
fn extract_tool_call_parts(raw: &str, parts: &mut Vec<serde_json::Value>) {
    let Ok(tool_calls) = serde_json::from_str::<Vec<ToolCall>>(raw) else {
        return;
    };
    for tool_call in tool_calls {
        let mut fields = tool_call.fields;
        fields.insert(
            "type".to_owned(),
            serde_json::Value::String("tool_call".to_owned()),
        );
        parts.push(serde_json::Value::Object(fields));
    }
}

/// Gets a string attribute value as an owned String.
fn get_str<T: AttributesLike>(attributes: &T, key: &str) -> Option<String> {
    let annotated = attributes.as_object().get(key)?;
    let value = annotated.value()?;
    value.as_str().map(|s| s.to_owned())
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::Attributes;

    use super::*;

    fn make_attributes(pairs: &[(&str, &str)]) -> Attributes {
        let mut attrs = Attributes::new();
        for (key, value) in pairs {
            attrs.insert(key.to_string(), value.to_string());
        }
        attrs
    }

    fn get_string(attributes: &Attributes, key: &str) -> Option<String> {
        attributes.get_value(key)?.as_str().map(|s| s.to_owned())
    }

    fn parse_json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
    }

    mod request_messages {
        use super::*;

        #[test]
        fn string_content_to_parts() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":"hello"}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "user", "parts": [{"type": "text", "content": "hello"}]}]),
            );
            assert!(get_string(&attrs, GEN_AI__REQUEST__MESSAGES).is_none());
        }

        #[test]
        fn array_content_to_parts() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"text","text":"hello"}]}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "user", "parts": [{"type": "text", "text": "hello", "content": "hello"}]}]),
            );
        }

        #[test]
        fn does_not_overwrite_existing() {
            let existing = r#"[{"role":"user","parts":[{"type":"text","content":"existing"}]}]"#;
            let mut attrs = make_attributes(&[
                (GEN_AI__INPUT__MESSAGES, existing),
                (
                    GEN_AI__REQUEST__MESSAGES,
                    r#"[{"role":"user","content":"ignored"}]"#,
                ),
            ]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(parse_json(&result), parse_json(existing));
            assert!(get_string(&attrs, GEN_AI__REQUEST__MESSAGES).is_none());
        }

        #[test]
        fn preserves_extra_fields() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"system","content":"be helpful","metadata":{"key":"value"}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            let parsed = parse_json(&result);
            assert_eq!(parsed[0]["role"], "system");
            assert_eq!(parsed[0]["metadata"]["key"], "value");
            assert!(parsed[0].get("content").is_none());
        }

        #[test]
        fn messages_with_metadata() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"system","content":"You are a helpful assistant.","response_metadata":{}},{"role":"user","content":"What is the capital of France?","response_metadata":{}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            let parsed = parse_json(&result);
            assert_eq!(parsed[0]["role"], "system");
            assert_eq!(parsed[0]["response_metadata"], serde_json::json!({}));
            assert_eq!(
                parsed[0]["parts"],
                serde_json::json!([{"type": "text", "content": "You are a helpful assistant."}]),
            );
            assert!(parsed[0].get("content").is_none());
        }

        #[test]
        fn preserves_unconvertible_content() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","content":{"result":"ok"}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            // Object content doesn't match OldContent variants, so the message
            // is moved as-is since the whole array fails to parse as Vec<OldMessage>.
            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "tool", "content": {"result": "ok"}}]),
            );
        }

        #[test]
        fn no_messages_is_noop() {
            let mut attrs = make_attributes(&[]);
            transform_gen_ai(&mut attrs);
            assert!(get_string(&attrs, GEN_AI__INPUT__MESSAGES).is_none());
        }

        #[test]
        fn invalid_json_moves_as_is() {
            let mut attrs = make_attributes(&[(GEN_AI__REQUEST__MESSAGES, "not json")]);

            transform_gen_ai(&mut attrs);

            assert_eq!(
                get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap(),
                "not json"
            );
            assert!(get_string(&attrs, GEN_AI__REQUEST__MESSAGES).is_none());
        }
    }

    mod response_to_output {
        use super::*;

        #[test]
        fn plain_text() {
            let mut attrs = make_attributes(&[(GEN_AI__RESPONSE__TEXT, "hello")]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"type": "text", "content": "hello"}
                ]}]),
            );
            assert!(get_string(&attrs, GEN_AI__RESPONSE__TEXT).is_none());
        }

        #[test]
        fn json_string() {
            let mut attrs = make_attributes(&[(GEN_AI__RESPONSE__TEXT, r#""hello world""#)]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"type": "text", "content": "hello world"}
                ]}]),
            );
        }

        #[test]
        fn array_of_strings() {
            let mut attrs = make_attributes(&[(GEN_AI__RESPONSE__TEXT, r#"["hello","world"]"#)]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"type": "text", "content": "hello"},
                    {"type": "text", "content": "world"}
                ]}]),
            );
        }

        #[test]
        fn object_with_content() {
            let mut attrs = make_attributes(&[(GEN_AI__RESPONSE__TEXT, r#"{"content":"hello"}"#)]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"type": "text", "content": "hello"}
                ]}]),
            );
        }

        #[test]
        fn array_of_objects_with_content() {
            let mut attrs = make_attributes(&[(
                GEN_AI__RESPONSE__TEXT,
                r#"[{"content":"hello"},{"content":"world"}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"type": "text", "content": "hello"},
                    {"type": "text", "content": "world"}
                ]}]),
            );
        }

        #[test]
        fn text_and_tool_calls() {
            let mut attrs = make_attributes(&[
                (GEN_AI__RESPONSE__TEXT, "hello"),
                (
                    GEN_AI__RESPONSE__TOOL_CALLS,
                    r#"[{"id":"call_1","name":"weather","arguments":{}}]"#,
                ),
            ]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"type": "text", "content": "hello"},
                    {"id": "call_1", "name": "weather", "arguments": {}, "type": "tool_call"}
                ]}]),
            );
            assert!(get_string(&attrs, GEN_AI__RESPONSE__TEXT).is_none());
            assert!(get_string(&attrs, GEN_AI__RESPONSE__TOOL_CALLS).is_none());
        }

        #[test]
        fn tool_calls_only() {
            let mut attrs = make_attributes(&[(
                GEN_AI__RESPONSE__TOOL_CALLS,
                r#"[{"id":"call_1","name":"weather","arguments":{}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "assistant", "parts": [
                    {"id": "call_1", "name": "weather", "arguments": {}, "type": "tool_call"}
                ]}]),
            );
        }

        #[test]
        fn does_not_overwrite_existing() {
            let existing =
                r#"[{"role":"assistant","parts":[{"type":"text","content":"existing"}]}]"#;
            let mut attrs = make_attributes(&[
                (GEN_AI__OUTPUT__MESSAGES, existing),
                (GEN_AI__RESPONSE__TEXT, "ignored"),
            ]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(parse_json(&result), parse_json(existing));
            assert!(get_string(&attrs, GEN_AI__RESPONSE__TEXT).is_none());
        }

        #[test]
        fn no_response_attributes_is_noop() {
            let mut attrs = make_attributes(&[]);
            transform_gen_ai(&mut attrs);
            assert!(get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).is_none());
        }
    }
}
