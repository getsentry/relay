//! Contains helper functions to convert generic browser reports into logs.
//!
//! Browser reports are sent through the [Reporting API](https://developer.mozilla.org/en-US/docs/Web/API/Reporting_API).
//! Report types with a dedicated conversion, such as CSP ([`super::csp`]) and NEL ([`super::nel`]),
//! should be converted with their specific functions. This module handles all other report types,
//! for example deprecation, intervention and crash reports.

use chrono::{DateTime, Duration, Utc};
use relay_conventions::attributes::{
    BROWSER__REPORT__TYPE, SENTRY__ORIGIN, URL__DOMAIN, URL__FULL,
};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, Timestamp, TraceId};
use relay_protocol::Annotated;
use serde::Deserialize;
use serde_json::Value;
use url::Url;

/// Maximum number of body fields converted into attributes.
const MAX_BODY_ATTRIBUTES: usize = 32;
/// Maximum length of an attribute key derived from a body field.
const MAX_KEY_LEN: usize = 64;
/// Maximum length in bytes of a single string attribute value.
///
/// Reports such as crash reports may contain large call stacks, which are truncated to this size.
const MAX_VALUE_LEN: usize = 8 * 1024;

/// Prefix of attributes derived from fields of the report body.
// TODO: This attribute must be defined in `sentry-conventions` before this ships.
const BODY_ATTRIBUTE_PREFIX: &str = "browser.report.body.";

/// A single report sent through the Reporting API.
///
/// See <https://w3c.github.io/reporting/#serialize-reports>.
#[derive(Debug, Deserialize)]
struct BrowserReport {
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    age: Option<Value>,
    #[serde(default)]
    body: Option<Value>,
}

/// Creates an [`OurLog`] from a single JSON encoded browser report.
pub fn create_log(report: &[u8], received_at: DateTime<Utc>) -> Result<OurLog, serde_json::Error> {
    create_log_with_trace_id(report, received_at, None)
}

/// Creates an [`OurLog`] from a single JSON encoded browser report with an optional trace ID.
///
/// If `trace_id` is `None`, a random one will be generated.
pub fn create_log_with_trace_id(
    report: &[u8],
    received_at: DateTime<Utc>,
    trace_id: Option<TraceId>,
) -> Result<OurLog, serde_json::Error> {
    let BrowserReport { ty, url, age, body } = serde_json::from_slice(report)?;

    let body = match body {
        Some(Value::Object(body)) => body,
        _ => Default::default(),
    };

    let timestamp = age
        .as_ref()
        .and_then(Value::as_i64)
        .and_then(|age| received_at.checked_sub_signed(Duration::milliseconds(age)))
        .unwrap_or(received_at);

    let message = match (ty.as_str(), body.get("message"), body.get("reason")) {
        (_, Some(Value::String(message)), _) if !message.is_empty() => truncate(message),
        ("crash", _, Some(Value::String(reason))) => format!("Browser crash: {}", truncate(reason)),
        ("crash", _, _) => "Browser crash".to_owned(),
        (ty, _, _) => format!("Browser report: {}", truncate(ty)),
    };

    let level = match ty.as_str() {
        "crash" => OurLogLevel::Error,
        "deprecation" => OurLogLevel::Info,
        _ => OurLogLevel::Warn,
    };

    let mut attributes = Attributes::default();

    let origin = match is_simple_type(&ty) {
        true => format!("auto.http.browser_report.{ty}"),
        false => "auto.http.browser_report".to_owned(),
    };
    attributes.insert(SENTRY__ORIGIN, origin);
    attributes.insert(BROWSER__REPORT__TYPE, truncate(&ty));

    if let Some(url) = url.filter(|url| !url.is_empty()) {
        if let Some(domain) = Url::parse(&url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
        {
            attributes.insert(URL__DOMAIN, domain);
        }
        attributes.insert(URL__FULL, truncate(&url));
    }

    for (key, value) in body.iter().take(MAX_BODY_ATTRIBUTES) {
        let Some(key) = attribute_key(key) else {
            continue;
        };

        match value {
            Value::String(value) if !value.is_empty() => attributes.insert(key, truncate(value)),
            Value::Bool(value) => attributes.insert(key, *value),
            Value::Number(value) => match value.as_i64() {
                Some(value) => attributes.insert(key, value),
                None => {
                    if let Some(value) = value.as_f64() {
                        attributes.insert(key, value);
                    }
                }
            },
            // Nested values are not supported, they would result in an unbounded amount of
            // attributes. Report types which need them must get a dedicated conversion.
            _ => {}
        }
    }

    Ok(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(trace_id.unwrap_or_else(TraceId::random)),
        level: Annotated::new(level),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}

/// Returns `true` if the report type is safe to be used as part of an attribute value.
fn is_simple_type(ty: &str) -> bool {
    !ty.is_empty()
        && ty.len() <= MAX_KEY_LEN
        && ty
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Converts the name of a body field into an attribute key.
///
/// Field names are converted to snake case, for example `sourceFile` becomes
/// `browser.report.body.source_file`. Returns `None` for field names which cannot be converted.
fn attribute_key(field: &str) -> Option<String> {
    let chars: Vec<char> = field.chars().collect();
    if chars.is_empty() || chars.len() > MAX_KEY_LEN {
        return None;
    }

    let mut key = String::with_capacity(BODY_ATTRIBUTE_PREFIX.len() + field.len() + 4);
    key.push_str(BODY_ATTRIBUTE_PREFIX);

    for (i, &c) in chars.iter().enumerate() {
        match c {
            'A'..='Z' => {
                let prev = i.checked_sub(1).map(|i| chars[i]);
                let next = chars.get(i + 1);
                // Start a new word on `fooBar` and on the last capital of an acronym like `URLFoo`.
                let new_word = match prev {
                    Some(p) if p.is_ascii_lowercase() || p.is_ascii_digit() => true,
                    Some(p) if p.is_ascii_uppercase() => next.is_some_and(char::is_ascii_lowercase),
                    _ => false,
                };
                if new_word && !key.ends_with('_') {
                    key.push('_');
                }
                key.push(c.to_ascii_lowercase());
            }
            'a'..='z' | '0'..='9' => key.push(c),
            '_' | '-' => key.push('_'),
            _ => return None,
        }
    }

    Some(key)
}

/// Truncates a string to at most [`MAX_VALUE_LEN`] bytes, respecting char boundaries.
fn truncate(value: &str) -> String {
    if value.len() <= MAX_VALUE_LEN {
        return value.to_owned();
    }

    let mut end = MAX_VALUE_LEN;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    fn received_at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2021-04-26T08:00:05+00:00")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn trace_id() -> TraceId {
        "de4e189601a342c6bee991645300852e".parse().unwrap()
    }

    fn create(json: &str) -> OurLog {
        create_log_with_trace_id(json.as_bytes(), received_at(), Some(trace_id())).unwrap()
    }

    #[test]
    fn test_attribute_key() {
        assert_eq!(
            attribute_key("sourceFile").as_deref(),
            Some("browser.report.body.source_file")
        );
        assert_eq!(
            attribute_key("is_top_level").as_deref(),
            Some("browser.report.body.is_top_level")
        );
        assert_eq!(
            attribute_key("anticipated-removal").as_deref(),
            Some("browser.report.body.anticipated_removal")
        );
        assert_eq!(
            attribute_key("blockedURL").as_deref(),
            Some("browser.report.body.blocked_url")
        );
        assert_eq!(
            attribute_key("XMLHttpRequest").as_deref(),
            Some("browser.report.body.xml_http_request")
        );
        assert_eq!(attribute_key(""), None);
        assert_eq!(attribute_key("foo.bar"), None);
        assert_eq!(attribute_key(&"a".repeat(MAX_KEY_LEN + 1)), None);
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("foo"), "foo");
        let long = "ä".repeat(MAX_VALUE_LEN);
        let truncated = truncate(&long);
        assert!(truncated.len() <= MAX_VALUE_LEN);
        assert!(long.starts_with(&truncated));
    }

    #[test]
    fn test_create_log_deprecation() {
        let log = create(
            r#"{
                "type": "deprecation",
                "age": 5000,
                "url": "https://example.com/page",
                "user_agent": "Mozilla/5.0",
                "body": {
                    "id": "websql",
                    "anticipatedRemoval": "2020-01-01",
                    "lineNumber": 1234,
                    "columnNumber": 42,
                    "sourceFile": "https://example.com/index.js",
                    "message": "WebSQL is deprecated and will be removed in Chrome 97 around January 2020"
                }
            }"#,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_crash() {
        let log = create(
            r#"{
                "type": "crash",
                "age": 0,
                "url": "https://example.com/page",
                "body": {
                    "reason": "unresponsive",
                    "stack": "SyntaxError: foo\n at bar (https://example.com/index.js:1:2)",
                    "is_top_level": true,
                    "page_visibility": "visible"
                }
            }"#,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_unknown_type() {
        let log = create(
            r#"{
                "type": "Some Future Report!",
                "url": "not a url",
                "body": {
                    "nested": {"foo": "bar"},
                    "list": [1, 2, 3],
                    "ratio": 0.5,
                    "invalid.key": "dropped",
                    "empty": ""
                }
            }"#,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(log)));
    }

    #[test]
    fn test_create_log_limits_body_attributes() {
        let body = (0..100)
            .map(|i| format!(r#""field{i}": {i}"#))
            .collect::<Vec<_>>()
            .join(",");
        let log = create(&format!(
            r#"{{"type": "intervention", "body": {{{body}}}}}"#
        ));

        let attributes = log.attributes.value().unwrap();
        let body_attributes = attributes
            .0
            .keys()
            .filter(|key| key.starts_with(BODY_ATTRIBUTE_PREFIX))
            .count();
        assert_eq!(body_attributes, MAX_BODY_ATTRIBUTES);
    }

    #[test]
    fn test_create_log_missing_type() {
        let result = create_log(br#"{"body": {}}"#, received_at());
        assert!(result.is_err());
    }
}
