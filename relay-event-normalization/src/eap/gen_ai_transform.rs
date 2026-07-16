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

use super::attribute_like::{AttributeLike, AttributesLike};

/// Applies gen_ai attribute transformations.
///
/// Reads from deprecated attribute keys, reshapes the values, writes to the
/// canonical keys, and removes the deprecated keys.
pub(crate) fn transform_gen_ai<T: AttributesLike>(attributes: &mut T) {
    transform_request_messages(attributes);
    transform_response_to_output_messages(attributes);
}

/// Transforms `gen_ai.request.messages` → `gen_ai.input.messages`.
///
/// Each message's `content` field is converted into a `parts` array:
/// - String content → `[{"type": "text", "content": "<value>"}]`
/// - Array content → kept, with `text` copied to `content` on each part if missing
/// - Other/missing content → message left unchanged
///
/// If the value isn't a valid JSON array of message objects, it is moved as-is.
fn transform_request_messages<T: AttributesLike>(attributes: &mut T) {
    if attributes.contains_key(GEN_AI__INPUT__MESSAGES) {
        attributes.remove(GEN_AI__REQUEST__MESSAGES);
        return;
    }

    let Some(raw) = get_str(attributes, GEN_AI__REQUEST__MESSAGES) else {
        return;
    };

    let Ok(messages) = serde_json::from_str::<Vec<serde_json::Value>>(&raw) else {
        // Not a JSON array — move as-is (just a key rename).
        if let Some(attr) = attributes.remove(GEN_AI__REQUEST__MESSAGES) {
            attributes.insert(GEN_AI__INPUT__MESSAGES.to_owned(), attr);
        }
        return;
    };

    let transformed: Vec<serde_json::Value> = messages
        .into_iter()
        .map(|mut message| {
            if let Some(parts) = content_to_parts(message.get("content"))
                && let Some(obj) = message.as_object_mut()
            {
                obj.remove("content");
                obj.insert("parts".to_owned(), parts);
            }
            message
        })
        .collect();

    if let Ok(json) = serde_json::to_string(&transformed) {
        attributes.insert(
            GEN_AI__INPUT__MESSAGES.to_owned(),
            T::Value::from(json).into(),
        );
    }
    attributes.remove(GEN_AI__REQUEST__MESSAGES);
}

/// Converts a message's `content` field into the new `parts` format.
fn content_to_parts(content: Option<&serde_json::Value>) -> Option<serde_json::Value> {
    match content? {
        serde_json::Value::String(s) => Some(serde_json::json!([
            {"type": "text", "content": s}
        ])),
        serde_json::Value::Array(items) => {
            let parts: Vec<serde_json::Value> = items
                .iter()
                .map(|part| {
                    let mut part = part.clone();
                    if let Some(obj) = part.as_object_mut()
                        && !obj.contains_key("content")
                        && let Some(text) = obj.get("text").cloned()
                    {
                        obj.insert("content".to_owned(), text);
                    }
                    part
                })
                .collect();
            Some(serde_json::Value::Array(parts))
        }
        // Other types (objects for tool messages, etc.) — leave unchanged.
        _ => None,
    }
}

/// Transforms `gen_ai.response.text` + `gen_ai.response.tool_calls` → `gen_ai.output.messages`.
///
/// Builds one assistant message with parts from both sources.
///
/// `gen_ai.response.text` can be:
/// - A plain string: `"hello"`
/// - A JSON string: `"\"hello\""`
/// - A JSON array of strings: `["hello", "world"]`
/// - A JSON object with content: `{"content": "hello"}`
/// - A JSON array of objects with content: `[{"content": "hello"}]`
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
        response_text_to_parts(text, &mut parts);
    }

    if let Some(ref raw) = tool_calls_raw {
        tool_calls_to_parts(raw, &mut parts);
    }

    // Always clean up deprecated keys, even if no parts could be extracted.
    attributes.remove(GEN_AI__RESPONSE__TEXT);
    attributes.remove(GEN_AI__RESPONSE__TOOL_CALLS);

    if parts.is_empty() {
        return;
    }

    let output = serde_json::json!([{"role": "assistant", "parts": parts}]);
    if let Ok(json) = serde_json::to_string(&output) {
        attributes.insert(
            GEN_AI__OUTPUT__MESSAGES.to_owned(),
            T::Value::from(json).into(),
        );
    }
}

/// Extracts text parts from a `gen_ai.response.text` value.
fn response_text_to_parts(raw: &str, parts: &mut Vec<serde_json::Value>) {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) else {
        // Not valid JSON — treat the entire raw string as plain text.
        parts.push(serde_json::json!({"type": "text", "content": raw}));
        return;
    };

    match parsed {
        serde_json::Value::String(s) => {
            parts.push(serde_json::json!({"type": "text", "content": s}));
        }
        serde_json::Value::Array(items) => {
            for item in items {
                match item {
                    serde_json::Value::String(s) => {
                        parts.push(serde_json::json!({"type": "text", "content": s}));
                    }
                    serde_json::Value::Object(ref obj) => {
                        if let Some(content) = obj.get("content") {
                            parts.push(serde_json::json!({"type": "text", "content": content}));
                        }
                    }
                    _ => {}
                }
            }
        }
        serde_json::Value::Object(ref obj) => {
            if let Some(content) = obj.get("content") {
                parts.push(serde_json::json!({"type": "text", "content": content}));
            }
        }
        _ => {
            // Other JSON primitives — use raw string as text.
            parts.push(serde_json::json!({"type": "text", "content": raw}));
        }
    }
}

/// Extracts tool_call parts from a `gen_ai.response.tool_calls` value.
fn tool_calls_to_parts(raw: &str, parts: &mut Vec<serde_json::Value>) {
    let Ok(tool_calls) = serde_json::from_str::<Vec<serde_json::Value>>(raw) else {
        return;
    };
    for mut tool_call in tool_calls {
        if let Some(obj) = tool_call.as_object_mut() {
            obj.insert(
                "type".to_owned(),
                serde_json::Value::String("tool_call".to_owned()),
            );
        }
        parts.push(tool_call);
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
    use relay_protocol::Annotated;

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
            assert_eq!(parsed[1]["role"], "user");
            assert_eq!(
                parsed[1]["parts"],
                serde_json::json!([{"type": "text", "content": "What is the capital of France?"}]),
            );
        }

        #[test]
        fn preserves_unconvertible_content() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","content":{"result":"ok"}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

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

    /// Integration tests: SpanV1 → normalize_attribute_names → normalize_ai (with transform).
    mod integration {
        use relay_event_schema::protocol::{Span as SpanV1, SpanData};

        use super::*;
        use crate::eap;

        #[test]
        fn span_v1_request_messages() {
            let span_v1 = Annotated::<SpanV1>::from_json(
                r#"{
                "data": {
                    "gen_ai.request.messages": "[{\"role\":\"user\",\"content\":\"hello\"}]",
                    "gen_ai.request.model": "gpt-4o",
                    "sentry.op": "gen_ai.generate_text"
                }
            }"#,
            )
            .unwrap();

            let mut data = span_v1.value().unwrap().data.clone();
            let data = data.get_or_insert_with(SpanData::default);
            transform_gen_ai(data);

            let result = data.get_str(GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(result),
                serde_json::json!([{"role": "user", "parts": [{"type": "text", "content": "hello"}]}]),
            );
            assert!(data.get_str(GEN_AI__REQUEST__MESSAGES).is_none());
        }

        #[test]
        fn span_v1_response_text() {
            let span_v1 = Annotated::<SpanV1>::from_json(
                r#"{
                "data": {
                    "gen_ai.response.text": "hello",
                    "gen_ai.request.model": "gpt-4o",
                    "sentry.op": "gen_ai.generate_text"
                }
            }"#,
            )
            .unwrap();

            let mut data = span_v1.value().unwrap().data.clone();
            let data = data.get_or_insert_with(SpanData::default);
            transform_gen_ai(data);

            let result = data.get_str(GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(result),
                serde_json::json!([{"role": "assistant", "parts": [{"type": "text", "content": "hello"}]}]),
            );
            assert!(data.get_str(GEN_AI__RESPONSE__TEXT).is_none());
        }

        #[test]
        fn span_v2_deprecated_keys() {
            let mut attrs = make_attributes(&[
                (
                    GEN_AI__REQUEST__MESSAGES,
                    r#"[{"role":"user","content":"hi"}]"#,
                ),
                (GEN_AI__RESPONSE__TEXT, "bye"),
                (SENTRY__OP, "gen_ai.generate_text"),
            ]);

            transform_gen_ai(&mut attrs);
            eap::normalize_attribute_names(&mut Annotated::new(attrs.clone()));

            let input = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&input),
                serde_json::json!([{"role": "user", "parts": [{"type": "text", "content": "hi"}]}]),
            );

            let output = get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&output),
                serde_json::json!([{"role": "assistant", "parts": [{"type": "text", "content": "bye"}]}]),
            );

            assert!(get_string(&attrs, GEN_AI__REQUEST__MESSAGES).is_none());
            assert!(get_string(&attrs, GEN_AI__RESPONSE__TEXT).is_none());
        }
    }
}
