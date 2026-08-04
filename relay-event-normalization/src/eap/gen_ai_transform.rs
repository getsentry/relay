//! Transforms deprecated gen_ai attributes into their normalized replacements.
//!
//! These transformations go beyond simple renaming — they reshape attribute values
//! to match the new schema. For the transformation specifications, see:
//! <https://github.com/getsentry/sentry-conventions/tree/main/model/attribute_transformations>
//!
//! Attributes with `_status: "transform"` in sentry-conventions produce
//! `WriteBehavior::CurrentName`, so [`super::normalize_attribute_names`] leaves
//! them alone — the full move-and-reshape is handled here.

// This module intentionally reads deprecated attribute keys to transform their values.
#![allow(deprecated)]

use relay_conventions::attributes::*;
use serde::{Deserialize, Serialize};

use super::attribute_like::{AttributeLike, AttributesLike};

// --- SDK input models (what SDKs send) ---

#[derive(Deserialize)]
struct RequestMessage {
    role: Option<String>,
    #[serde(default)]
    name: Option<String>,
    content: Option<RequestMessageContent>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RequestMessageContent {
    String(String),
    Array(Vec<SdkContentItem>),
    Single(SdkContentItem),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SdkContentItem {
    Tagged(SdkPart),
    Untagged(SdkContentObject),
    Generic(GenericPart),
}

impl From<SdkContentItem> for InputPart {
    fn from(item: SdkContentItem) -> Self {
        match item {
            SdkContentItem::Tagged(p) => p.into(),
            SdkContentItem::Untagged(o) => o.into(),
            SdkContentItem::Generic(g) => InputPart::Generic(g),
        }
    }
}

/// A part as sent by SDKs. Tagged by `type` with aliases for common SDK variants.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum SdkPart {
    #[serde(alias = "text")]
    Text(SdkTextPart),
    #[serde(alias = "tool_call")]
    ToolCall(SdkToolCallPart),
    #[serde(
        alias = "tool_call_response",
        alias = "function_call_output",
        alias = "tool-result"
    )]
    ToolCallResponse(SdkToolCallResponsePart),
    #[serde(alias = "server_tool_call")]
    ServerToolCall(SdkServerToolCallPart),
    #[serde(alias = "server_tool_call_response")]
    ServerToolCallResponse(SdkServerToolCallResponsePart),
    #[serde(alias = "blob", alias = "image")]
    Blob(SdkBlobPart),
    #[serde(alias = "file")]
    File(SdkFilePart),
    #[serde(alias = "uri", alias = "image_url")]
    Uri(SdkUriPart),
    #[serde(alias = "reasoning")]
    Reasoning(SdkReasoningPart),
    #[serde(alias = "compaction")]
    Compaction(SdkCompactionPart),
}

/// For `content: object` without a `type` tag (Google GenAI, etc.).
#[derive(Deserialize)]
#[serde(untagged)]
enum SdkContentObject {
    Text(SdkTextObject),
    InlineData(SdkInlineDataObject),
    ToolResult(SdkToolResultObject),
    Generic(GenericPart),
}

// SDK part structs — deserialize with aliases for field name variations.

/// `{ text }` or `{ content }` or `{ text, content }`
#[derive(Deserialize)]
struct SdkTextPart {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Deserialize)]
struct SdkToolCallPart {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    arguments: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct SdkToolCallResponsePart {
    #[serde(default, alias = "call_id", alias = "toolCallId")]
    id: Option<String>,
    #[serde(alias = "output")]
    response: serde_json::Value,
}

#[derive(Deserialize)]
struct SdkServerToolCallPart {
    #[serde(default)]
    id: Option<String>,
    name: String,
    server_tool_call: GenericPart,
}

#[derive(Deserialize)]
struct SdkServerToolCallResponsePart {
    #[serde(default)]
    id: Option<String>,
    server_tool_call_response: GenericPart,
}

/// Handles `{ content }`, `{ image }`, `{ data }`, and `{ source: { data, media_type } }`.
#[derive(Deserialize)]
struct SdkBlobPart {
    #[serde(default, alias = "mimeType", alias = "mediaType", alias = "media_type")]
    mime_type: Option<String>,
    #[serde(default)]
    modality: Option<String>,
    #[serde(default, alias = "image", alias = "data")]
    content: Option<String>,
    #[serde(default)]
    source: Option<SdkBlobSource>,
}

#[derive(Deserialize)]
struct SdkBlobSource {
    #[serde(default, alias = "media_type")]
    mime_type: Option<String>,
    data: String,
}

#[derive(Deserialize)]
struct SdkFilePart {
    #[serde(default, alias = "mimeType", alias = "mediaType")]
    mime_type: Option<String>,
    #[serde(default)]
    modality: Option<String>,
    #[serde(default)]
    file_id: Option<String>,
}

#[derive(Deserialize)]
struct SdkUriPart {
    #[serde(default, alias = "mimeType")]
    mime_type: Option<String>,
    #[serde(default)]
    modality: Option<String>,
    #[serde(default, alias = "url")]
    uri: Option<String>,
    #[serde(default)]
    image_url: Option<SdkImageUrl>,
}

#[derive(Deserialize)]
struct SdkImageUrl {
    url: String,
}

#[derive(Deserialize)]
struct SdkReasoningPart {
    content: String,
}

#[derive(Deserialize)]
struct SdkCompactionPart {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    content: Option<String>,
}

// Untagged object content variants (no `type` field).

/// `{ text: "..." }` (Google GenAI)
#[derive(Deserialize)]
struct SdkTextObject {
    text: String,
}

/// `{ inlineData: { mimeType, data } }` (Google GenAI)
#[derive(Deserialize)]
struct SdkInlineDataObject {
    #[serde(alias = "inlineData")]
    inline_data: SdkInlineData,
}

#[derive(Deserialize)]
struct SdkInlineData {
    #[serde(default, alias = "mimeType")]
    mime_type: Option<String>,
    data: String,
}

/// `{ toolCallId, toolName, output }` (Google GenAI)
#[derive(Deserialize)]
struct SdkToolResultObject {
    #[serde(alias = "toolCallId")]
    tool_call_id: String,
    #[serde(default, alias = "toolName")]
    tool_name: Option<String>,
    output: serde_json::Value,
}

/// Catch-all preserving all fields.
#[derive(Deserialize, Serialize)]
struct GenericPart {
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}

// --- OTel canonical output models ---

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InputPart {
    Text(TextPart),
    ToolCall(ToolCallPart),
    ToolCallResponse(ToolCallResponsePart),
    ServerToolCall(ServerToolCallPart),
    ServerToolCallResponse(ServerToolCallResponsePart),
    Blob(BlobPart),
    File(FilePart),
    Uri(UriPart),
    Reasoning(ReasoningPart),
    Compaction(CompactionPart),
    #[serde(untagged)]
    Generic(GenericPart),
}

#[derive(Serialize)]
struct TextPart {
    content: String,
}

#[derive(Serialize)]
struct ToolCallPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    arguments: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct ToolCallResponsePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    response: serde_json::Value,
}

#[derive(Serialize)]
struct ServerToolCallPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
    server_tool_call: GenericPart,
}

#[derive(Serialize)]
struct ServerToolCallResponsePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    server_tool_call_response: GenericPart,
}

#[derive(Serialize)]
struct BlobPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    modality: String,
    content: String,
}

#[derive(Serialize)]
struct FilePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    modality: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_id: Option<String>,
}

#[derive(Serialize)]
struct UriPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    modality: String,
    uri: String,
}

#[derive(Serialize)]
struct ReasoningPart {
    content: String,
}

#[derive(Serialize)]
struct CompactionPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

// --- Mapping from SDK models to OTel output ---

impl From<SdkPart> for InputPart {
    fn from(sdk: SdkPart) -> Self {
        match sdk {
            SdkPart::Text(p) => InputPart::Text(TextPart {
                content: p.content.or(p.text).unwrap_or_default(),
            }),
            SdkPart::ToolCall(p) => InputPart::ToolCall(ToolCallPart {
                id: p.id,
                name: p.name,
                arguments: p.arguments,
            }),
            SdkPart::ToolCallResponse(p) => InputPart::ToolCallResponse(ToolCallResponsePart {
                id: p.id,
                response: p.response,
            }),
            SdkPart::ServerToolCall(p) => InputPart::ServerToolCall(ServerToolCallPart {
                id: p.id,
                name: p.name,
                server_tool_call: p.server_tool_call,
            }),
            SdkPart::ServerToolCallResponse(p) => {
                InputPart::ServerToolCallResponse(ServerToolCallResponsePart {
                    id: p.id,
                    server_tool_call_response: p.server_tool_call_response,
                })
            }
            SdkPart::Blob(p) => {
                let (content, mime_type) = match p.source {
                    Some(src) => (src.data, src.mime_type.or(p.mime_type)),
                    None => (p.content.unwrap_or_default(), p.mime_type),
                };
                InputPart::Blob(BlobPart {
                    mime_type,
                    modality: p.modality.unwrap_or_else(|| "image".to_owned()),
                    content,
                })
            }
            SdkPart::File(p) => InputPart::File(FilePart {
                mime_type: p.mime_type,
                modality: p.modality.unwrap_or_else(|| "document".to_owned()),
                file_id: p.file_id,
            }),
            SdkPart::Uri(p) => {
                let uri = p
                    .uri
                    .or_else(|| p.image_url.map(|u| u.url))
                    .unwrap_or_default();
                InputPart::Uri(UriPart {
                    mime_type: p.mime_type,
                    modality: p.modality.unwrap_or_else(|| "image".to_owned()),
                    uri,
                })
            }
            SdkPart::Reasoning(p) => InputPart::Reasoning(ReasoningPart { content: p.content }),
            SdkPart::Compaction(p) => InputPart::Compaction(CompactionPart {
                id: p.id,
                content: p.content,
            }),
        }
    }
}

impl From<SdkContentObject> for InputPart {
    fn from(sdk: SdkContentObject) -> Self {
        match sdk {
            SdkContentObject::Text(p) => InputPart::Text(TextPart { content: p.text }),
            SdkContentObject::InlineData(p) => InputPart::Blob(BlobPart {
                mime_type: p.inline_data.mime_type,
                modality: "image".to_owned(),
                content: p.inline_data.data,
            }),
            SdkContentObject::ToolResult(p) => InputPart::ToolCallResponse(ToolCallResponsePart {
                id: Some(p.tool_call_id),
                response: p.output,
            }),
            SdkContentObject::Generic(p) => InputPart::Generic(p),
        }
    }
}

#[derive(Serialize)]
struct InputMessage {
    role: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parts: Vec<InputPart>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

// --- Response output models ---

#[derive(Deserialize)]
#[serde(untagged)]
enum ResponseText {
    String(String),
    Array(Vec<ResponseTextItem>),
    Object { content: serde_json::Value },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ResponseTextItem {
    String(String),
    Object { content: serde_json::Value },
}

#[derive(Serialize)]
struct OutputMessage {
    role: &'static str,
    parts: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct ToolCall {
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}

// --- Transformation logic ---

/// Applies gen_ai attribute transformations.
pub(super) fn transform_gen_ai<T: AttributesLike>(attributes: &mut T) {
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

    let Ok(messages) = serde_json::from_str::<Vec<RequestMessage>>(&raw) else {
        if let Some(attr) = attributes.remove(GEN_AI__REQUEST__MESSAGES) {
            attributes.insert(GEN_AI__INPUT__MESSAGES.to_owned(), attr);
        }
        return;
    };

    let new_messages: Vec<InputMessage> = messages
        .into_iter()
        .map(|msg| {
            let parts: Vec<InputPart> = match msg.content {
                Some(RequestMessageContent::String(s)) => {
                    vec![InputPart::Text(TextPart { content: s })]
                }
                Some(RequestMessageContent::Array(items)) => {
                    items.into_iter().map(InputPart::from).collect()
                }
                Some(RequestMessageContent::Single(item)) => vec![InputPart::from(item)],
                None => vec![],
            };

            InputMessage {
                role: msg.role.unwrap_or_default(),
                parts,
                name: msg.name,
            }
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

    let tool_calls_parsed = tool_calls_raw
        .as_deref()
        .is_none_or(|raw| extract_tool_call_parts(raw, &mut parts));

    // Clean up deprecated keys. Keep tool_calls if it couldn't be parsed.
    attributes.remove(GEN_AI__RESPONSE__TEXT);
    if tool_calls_parsed {
        attributes.remove(GEN_AI__RESPONSE__TOOL_CALLS);
    }

    if parts.is_empty() {
        return;
    }

    let output = [OutputMessage {
        role: "assistant",
        parts,
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
///
/// Returns `true` if the value was successfully parsed.
fn extract_tool_call_parts(raw: &str, parts: &mut Vec<serde_json::Value>) -> bool {
    let Ok(tool_calls) = serde_json::from_str::<Vec<ToolCall>>(raw) else {
        return false;
    };
    for tool_call in tool_calls {
        let mut fields = tool_call.fields;
        fields.insert(
            "type".to_owned(),
            serde_json::Value::String("tool_call".to_owned()),
        );
        parts.push(serde_json::Value::Object(fields));
    }
    true
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
            // `text` is accepted as alias for `content` and normalized.
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "user", "parts": [{"type": "text", "content": "hello"}]}]),
            );
        }

        #[test]
        fn array_content_with_content_field() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"text","content":"hello"}]}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "user", "parts": [{"type": "text", "content": "hello"}]}]),
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
        fn drops_extra_fields() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"system","content":"be helpful","metadata":{"key":"value"}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            let parsed = parse_json(&result);
            assert_eq!(parsed[0]["role"], "system");
            assert_eq!(
                parsed[0]["parts"],
                serde_json::json!([{"type": "text", "content": "be helpful"}]),
            );
            // Extra fields like `metadata` are dropped.
            assert!(parsed[0].get("metadata").is_none());
        }

        #[test]
        fn messages_with_metadata_drops_extra() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"system","content":"You are a helpful assistant.","response_metadata":{}},{"role":"user","content":"What is the capital of France?","response_metadata":{}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            let parsed = parse_json(&result);
            assert_eq!(parsed[0]["role"], "system");
            assert_eq!(
                parsed[0]["parts"],
                serde_json::json!([{"type": "text", "content": "You are a helpful assistant."}]),
            );
            // response_metadata is not part of ChatMessage schema.
            assert!(parsed[0].get("response_metadata").is_none());
        }

        #[test]
        fn object_content_as_generic_part() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","content":{"result":"ok"}}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "tool", "parts": [{"result": "ok"}]}]),
            );
        }

        #[test]
        fn preserves_name_field() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","name":"get_weather","content":"sunny"}]"#,
            )]);

            transform_gen_ai(&mut attrs);

            let result = get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap();
            assert_eq!(
                parse_json(&result),
                serde_json::json!([{"role": "tool", "name": "get_weather", "parts": [{"type": "text", "content": "sunny"}]}]),
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

        // --- SDK content: array variants ---

        #[test]
        fn sdk_text_with_text_field() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"text","text":"hello"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(
                result[0]["parts"][0],
                serde_json::json!({"type": "text", "content": "hello"})
            );
        }

        #[test]
        fn sdk_image_url_nested() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/img.png"}}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "uri");
            assert_eq!(result[0]["parts"][0]["uri"], "https://example.com/img.png");
            assert_eq!(result[0]["parts"][0]["modality"], "image");
        }

        #[test]
        fn sdk_image_with_image_field() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"image","image":"base64data"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "blob");
            assert_eq!(result[0]["parts"][0]["content"], "base64data");
            assert_eq!(result[0]["parts"][0]["modality"], "image");
        }

        #[test]
        fn sdk_image_with_mime_type() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"image","image":"base64data","mimeType":"image/png"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "blob");
            assert_eq!(result[0]["parts"][0]["content"], "base64data");
            assert_eq!(result[0]["parts"][0]["mime_type"], "image/png");
        }

        #[test]
        fn sdk_file_with_media_type_and_data() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"file","mediaType":"application/pdf","data":"base64pdf"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "file");
            assert_eq!(result[0]["parts"][0]["mime_type"], "application/pdf");
        }

        #[test]
        fn sdk_blob_canonical() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"blob","modality":"audio","mime_type":"audio/wav","content":"base64audio"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "blob");
            assert_eq!(result[0]["parts"][0]["modality"], "audio");
            assert_eq!(result[0]["parts"][0]["mime_type"], "audio/wav");
            assert_eq!(result[0]["parts"][0]["content"], "base64audio");
        }

        #[test]
        fn sdk_image_with_source_anthropic() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"base64jpg"}}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "blob");
            assert_eq!(result[0]["parts"][0]["content"], "base64jpg");
            assert_eq!(result[0]["parts"][0]["mime_type"], "image/jpeg");
        }

        #[test]
        fn sdk_image_with_content_field() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"image","content":"base64data"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "blob");
            assert_eq!(result[0]["parts"][0]["content"], "base64data");
        }

        #[test]
        fn sdk_function_call_output() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","content":[{"type":"function_call_output","call_id":"call_1","output":"sunny"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "tool_call_response");
            assert_eq!(result[0]["parts"][0]["id"], "call_1");
            assert_eq!(result[0]["parts"][0]["response"], "sunny");
        }

        #[test]
        fn sdk_tool_result_with_hyphen() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","content":[{"type":"tool-result","toolCallId":"call_2","toolName":"weather","output":"rainy"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "tool_call_response");
            assert_eq!(result[0]["parts"][0]["id"], "call_2");
            assert_eq!(result[0]["parts"][0]["response"], "rainy");
        }

        // --- SDK content: object variants (no type tag) ---

        #[test]
        fn sdk_google_text_object() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":{"text":"hello from google"}}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "text");
            assert_eq!(result[0]["parts"][0]["content"], "hello from google");
        }

        #[test]
        fn sdk_google_inline_data() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":{"inlineData":{"mimeType":"image/png","data":"base64img"}}}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "blob");
            assert_eq!(result[0]["parts"][0]["content"], "base64img");
            assert_eq!(result[0]["parts"][0]["mime_type"], "image/png");
        }

        #[test]
        fn sdk_google_tool_result_object() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"tool","content":{"toolCallId":"call_3","toolName":"weather","output":"cloudy"}}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(result[0]["parts"][0]["type"], "tool_call_response");
            assert_eq!(result[0]["parts"][0]["id"], "call_3");
            assert_eq!(result[0]["parts"][0]["response"], "cloudy");
        }

        // --- Mixed arrays ---

        #[test]
        fn sdk_mixed_text_and_image() {
            let mut attrs = make_attributes(&[(
                GEN_AI__REQUEST__MESSAGES,
                r#"[{"role":"user","content":[{"type":"text","text":"describe this"},{"type":"image","image":"base64data","mimeType":"image/png"}]}]"#,
            )]);
            transform_gen_ai(&mut attrs);
            let result = parse_json(&get_string(&attrs, GEN_AI__INPUT__MESSAGES).unwrap());
            assert_eq!(
                result[0]["parts"][0],
                serde_json::json!({"type": "text", "content": "describe this"})
            );
            assert_eq!(result[0]["parts"][1]["type"], "blob");
            assert_eq!(result[0]["parts"][1]["content"], "base64data");
            assert_eq!(result[0]["parts"][1]["mime_type"], "image/png");
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

        #[test]
        fn non_json_tool_calls_preserved() {
            let mut attrs = make_attributes(&[(GEN_AI__RESPONSE__TOOL_CALLS, "some_tool_calls")]);

            transform_gen_ai(&mut attrs);

            // Can't parse, so the deprecated key is kept and no output is created.
            assert_eq!(
                get_string(&attrs, GEN_AI__RESPONSE__TOOL_CALLS).unwrap(),
                "some_tool_calls"
            );
            assert!(get_string(&attrs, GEN_AI__OUTPUT__MESSAGES).is_none());
        }
    }
}
