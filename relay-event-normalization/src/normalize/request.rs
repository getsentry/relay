//! Normalization of the [`Request`] interface.
//!
//! See [`normalize_request`] for more information.

use std::sync::OnceLock;

use regex::Regex;
use relay_event_schema::processor::{self, ProcessingAction, ProcessingResult};
use relay_event_schema::protocol::{Cookies, Query, Request};
use relay_protocol::{Annotated, ErrorKind, Meta, Value};
use url::Url;

const ELLIPSIS: char = '\u{2026}';

fn normalize_url(request: &mut Request) {
    let url_string = match request.url.value_mut() {
        Some(url_string) => url_string,
        None => return,
    };

    // Special case: JavaScript SDK used to send an ellipsis character for
    // truncated URLs. Canonical URLs do not contain UTF-8 characters in
    // either the path, query string or fragment, so we replace it with
    // three dots (which is the behavior of other SDKs). This effectively
    // makes the string two characters longer, but it will be trimmed
    // again later if it is too long in the end.
    if url_string.ends_with(ELLIPSIS) {
        url_string.truncate(url_string.len() - ELLIPSIS.len_utf8());
        url_string.push_str("...");
    }

    match Url::parse(url_string) {
        Ok(mut url) => {
            // Separate the query string and fragment bits into dedicated fields. If
            // both the URL and the fields have been set, the fields take precedence.
            if request.query_string.value().is_none() {
                let query: Query = url.query_pairs().collect();
                if !query.is_empty() {
                    request.query_string.set_value(Some(query));
                }
            }

            if request.fragment.value().is_none() {
                request
                    .fragment
                    .set_value(url.fragment().map(str::to_string));
            }

            url.set_query(None);
            url.set_fragment(None);
            if url.as_str() != url_string {
                *url_string = url.into();
            }
        }
        Err(_) => {
            // The URL is invalid, but we can still apply heuristics to parse the query
            // string and put the fragment in its own field.
            if let Some(fragment_index) = url_string.find('#') {
                let fragment = &url_string[fragment_index + 1..];
                if !fragment.is_empty() && request.fragment.value().is_none() {
                    request.fragment.set_value(Some(fragment.to_owned()));
                }
                url_string.truncate(fragment_index);
            }

            if let Some(query_index) = url_string.find('?') {
                let query_string = &url_string[query_index + 1..];
                if !query_string.is_empty() && request.query_string.value().is_none() {
                    let query = Query::parse(query_string);
                    if !query.is_empty() {
                        request.query_string.set_value(Some(query));
                    }
                }
                url_string.truncate(query_index);
            }
        }
    };
}

#[allow(clippy::ptr_arg)] // normalize_method must be &mut String for `apply`.
fn normalize_method(method: &mut String, meta: &mut Meta) -> ProcessingResult {
    method.make_ascii_uppercase();

    static METHOD_RE: OnceLock<Regex> = OnceLock::new();
    let regex = METHOD_RE.get_or_init(|| Regex::new(r"^[A-Z\-_]{3,32}$").unwrap());

    if !meta.has_errors() && !regex.is_match(method) {
        meta.add_error(ErrorKind::InvalidData);
        return Err(ProcessingAction::DeleteValueSoft);
    }

    Ok(())
}

/// Decodes an urlencoded body.
fn urlencoded_from_str(raw: &str) -> Option<Value> {
    // Binary strings would be decoded, but we know url-encoded bodies are ASCII.
    if !raw.is_ascii() {
        return None;
    }

    // Avoid false positives with XML and partial JSON.
    if raw.starts_with("<?xml") || raw.starts_with('{') || raw.starts_with('[') {
        return None;
    }

    // serde_urlencoded always deserializes into `Value::Object`.
    let object = match serde_urlencoded::from_str(raw) {
        Ok(Value::Object(value)) => value,
        _ => return None,
    };

    // `serde_urlencoded` can decode any string with valid characters into an object. However, we
    // need to account for false-positives in the following cases:
    //  - An empty string "" is decoded as empty object
    //  - A string "foo" is decoded as {"foo": ""} (check for single empty value)
    //  - A base64 encoded string "dGU=" also decodes with a single empty value
    //  - A base64 encoded string "dA==" decodes as {"dA": "="} (check for single =)
    let is_valid = object.len() > 1
        || object
            .values()
            .next()
            .and_then(Annotated::<Value>::as_str)
            .is_some_and(|s| !matches!(s, "" | "="));

    if is_valid {
        Some(Value::Object(object))
    } else {
        None
    }
}

fn parse_raw_data(request: &Request) -> Option<(&'static str, Value)> {
    let raw = request.data.as_str()?;

    // TODO: Try to decode base64 first

    if let Ok(value) = serde_json::from_str(raw) {
        Some(("application/json", value))
    } else {
        urlencoded_from_str(raw).map(|value| ("application/x-www-form-urlencoded", value))
    }
}

fn normalize_data(request: &mut Request) {
    // Always derive the `inferred_content_type` from the request body, even if there is a
    // `Content-Type` header present. This value can technically be ingested (due to the schema) but
    // should always be overwritten in normalization. Only if inference fails, fall back to the
    // content type header.
    if let Some((content_type, parsed_data)) = parse_raw_data(request) {
        // Retain meta data on the body (e.g. trimming annotations) but remove anything on the
        // inferred content type.
        request.data.set_value(Some(parsed_data));
        request.inferred_content_type = Annotated::from(content_type.to_owned());
    } else {
        request.inferred_content_type = request
            .headers
            .value()
            .and_then(|headers| headers.get_header("Content-Type"))
            .map(|value| value.split(';').next().unwrap_or(value).to_owned())
            .into();
    }
}

fn normalize_cookies(request: &mut Request) {
    let headers = match request.headers.value_mut() {
        Some(headers) => headers,
        None => return,
    };

    if request.cookies.value().is_some() {
        headers.remove("Cookie");
        return;
    }

    let cookie_header = match headers.get_header("Cookie") {
        Some(header) => header,
        None => return,
    };

    if let Ok(new_cookies) = Cookies::parse(cookie_header) {
        request.cookies = Annotated::from(new_cookies);
        headers.remove("Cookie");
    }
}

/// Normalizes the [`Request`] interface.
///
/// This function applies the following normalization rules:
/// - The URL is truncated to 2048 characters.
/// - The query string and fragment are extracted into dedicated fields.
/// - The method is normalized to uppercase.
/// - The data is parsed as JSON or urlencoded and put into the `data` field.
/// - The `Content-Type` header is parsed and put into the `inferred_content_type` field.
/// - The `Cookie` header is parsed and put into the `cookies` field.
pub fn normalize_request(request: &mut Request) {
    let _ = processor::apply(&mut request.method, normalize_method);
    normalize_url(request);
    normalize_data(request);
    normalize_cookies(request);
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Headers, PairList};
    use relay_protocol::Object;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_url_truncation() {
        let mut request = Request {
            url: Annotated::new("http://example.com/path?foo#bar".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.url.as_str(), Some("http://example.com/path"));
    }

    #[test]
    fn test_url_truncation_reversed() {
        let mut request = Request {
            // The query string is empty and the fragment is "foo?bar" here
            url: Annotated::new("http://example.com/path#foo?bar".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.url.as_str(), Some("http://example.com/path"));
    }

    #[test]
    fn test_url_with_ellipsis() {
        let mut request = Request {
            url: Annotated::new("http://example.com/path…".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.url.as_str(), Some("http://example.com/path..."));
    }

    #[test]
    fn test_url_with_qs_and_fragment() {
        let mut request = Request {
            url: Annotated::new("http://example.com/path?some=thing#else".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(
            request,
            Request {
                url: Annotated::new("http://example.com/path".to_owned()),
                query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                    Annotated::new("some".to_owned()),
                    Annotated::new("thing".to_owned().into()),
                )),]))),
                fragment: Annotated::new("else".to_owned()),
                ..Request::default()
            }
        );
    }

    #[test]
    fn test_url_only_path() {
        let mut request = Request {
            url: Annotated::from("metamask/popup.html#".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(
            request,
            Request {
                url: Annotated::new("metamask/popup.html".to_owned()),
                ..Request::default()
            }
        );
    }

    #[test]
    fn test_url_punycoded() {
        let mut request = Request {
            url: Annotated::new("http://göögle.com/".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(
            request,
            Request {
                url: Annotated::new("http://xn--ggle-5qaa.com/".to_owned()),
                ..Request::default()
            }
        );
    }

    #[test]
    fn test_url_precedence() {
        let mut request = Request {
            url: Annotated::new("http://example.com/path?completely=different#stuff".to_owned()),
            query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                Annotated::new("some".to_owned()),
                Annotated::new("thing".to_owned().into()),
            ))]))),
            fragment: Annotated::new("else".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(
            request,
            Request {
                url: Annotated::new("http://example.com/path".to_owned()),
                query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                    Annotated::new("some".to_owned()),
                    Annotated::new("thing".to_owned().into()),
                )),]))),
                fragment: Annotated::new("else".to_owned()),
                ..Request::default()
            }
        );
    }

    #[test]
    fn test_query_string_empty_value() {
        let mut request = Request {
            url: Annotated::new("http://example.com/path?some".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(
            request,
            Request {
                url: Annotated::new("http://example.com/path".to_owned()),
                query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                    Annotated::new("some".to_owned()),
                    Annotated::new("".to_owned().into()),
                )),]))),
                ..Request::default()
            }
        );
    }

    #[test]
    fn test_cookies_in_header() {
        let mut request = Request {
            url: Annotated::new("http://example.com".to_owned()),
            headers: Annotated::new(Headers(PairList(vec![Annotated::new((
                Annotated::new("Cookie".to_owned().into()),
                Annotated::new("a=b;c=d".to_owned().into()),
            ))]))),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(
            request.cookies,
            Annotated::new(Cookies(PairList(vec![
                Annotated::new((
                    Annotated::new("a".to_owned()),
                    Annotated::new("b".to_owned()),
                )),
                Annotated::new((
                    Annotated::new("c".to_owned()),
                    Annotated::new("d".to_owned()),
                )),
            ])))
        );

        assert_eq!(request.headers.value().unwrap().get_header("Cookie"), None);
    }

    #[test]
    fn test_cookies_in_header_dont_override_cookies() {
        let mut request = Request {
            url: Annotated::new("http://example.com".to_owned()),
            headers: Annotated::new(Headers(
                vec![Annotated::new((
                    Annotated::new("Cookie".to_owned().into()),
                    Annotated::new("a=b;c=d".to_owned().into()),
                ))]
                .into(),
            )),
            cookies: Annotated::new(Cookies(PairList(vec![Annotated::new((
                Annotated::new("foo".to_owned()),
                Annotated::new("bar".to_owned()),
            ))]))),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(
            request.cookies,
            Annotated::new(Cookies(PairList(vec![Annotated::new((
                Annotated::new("foo".to_owned()),
                Annotated::new("bar".to_owned()),
            ))])))
        );

        // Cookie header is removed when explicit cookies are given
        assert_eq!(request.headers.value().unwrap().get_header("Cookie"), None);
    }

    #[test]
    fn test_method_invalid() {
        let mut request = Request {
            method: Annotated::new("!!!!".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(request.method.value(), None);
    }

    #[test]
    fn test_method_valid() {
        let mut request = Request {
            method: Annotated::new("POST".to_owned()),
            ..Request::default()
        };

        normalize_request(&mut request);

        assert_eq!(request.method.as_str(), Some("POST"));
    }

    #[test]
    fn test_infer_json() {
        let mut request = Request {
            data: Annotated::from(Value::String(r#"{"foo":"bar"}"#.to_owned())),
            ..Request::default()
        };

        let mut expected_value = Object::new();
        expected_value.insert(
            "foo".to_owned(),
            Annotated::from(Value::String("bar".into())),
        );

        normalize_request(&mut request);
        assert_eq!(
            request.inferred_content_type.as_str(),
            Some("application/json")
        );
        assert_eq!(request.data.value(), Some(&Value::Object(expected_value)));
    }

    #[test]
    fn test_broken_json_with_fallback() {
        let mut request = Request {
            data: Annotated::from(Value::String(r#"{"foo":"b"#.to_owned())),
            headers: Annotated::from(Headers(PairList(vec![Annotated::new((
                Annotated::new("Content-Type".to_owned().into()),
                Annotated::new("text/plain; encoding=utf-8".to_owned().into()),
            ))]))),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.inferred_content_type.as_str(), Some("text/plain"));
        assert_eq!(request.data.as_str(), Some(r#"{"foo":"b"#));
    }

    #[test]
    fn test_broken_json_without_fallback() {
        let mut request = Request {
            data: Annotated::from(Value::String(r#"{"foo":"b"#.to_owned())),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.inferred_content_type.value(), None);
        assert_eq!(request.data.as_str(), Some(r#"{"foo":"b"#));
    }

    #[test]
    fn test_infer_url_encoded() {
        let mut request = Request {
            data: Annotated::from(Value::String(r#"foo=bar"#.to_owned())),
            ..Request::default()
        };

        let mut expected_value = Object::new();
        expected_value.insert(
            "foo".to_owned(),
            Annotated::from(Value::String("bar".into())),
        );

        normalize_request(&mut request);
        assert_eq!(
            request.inferred_content_type.as_str(),
            Some("application/x-www-form-urlencoded")
        );
        assert_eq!(request.data.value(), Some(&Value::Object(expected_value)));
    }

    #[test]
    fn test_infer_url_false_positive() {
        let mut request = Request {
            data: Annotated::from(Value::String("dGU=".to_owned())),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.inferred_content_type.value(), None);
        assert_eq!(request.data.as_str(), Some("dGU="));
    }

    #[test]
    fn test_infer_url_encoded_base64() {
        let mut request = Request {
            data: Annotated::from(Value::String("dA==".to_owned())),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.inferred_content_type.value(), None);
        assert_eq!(request.data.as_str(), Some("dA=="));
    }

    #[test]
    fn test_infer_xml() {
        let mut request = Request {
            data: Annotated::from(Value::String("<?xml version=\"1.0\" ?>".to_owned())),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.inferred_content_type.value(), None);
        assert_eq!(request.data.as_str(), Some("<?xml version=\"1.0\" ?>"));
    }

    #[test]
    fn test_infer_binary() {
        let mut request = Request {
            data: Annotated::from(Value::String("\u{001f}1\u{0000}\u{0000}".to_owned())),
            ..Request::default()
        };

        normalize_request(&mut request);
        assert_eq!(request.inferred_content_type.value(), None);
        assert_eq!(request.data.as_str(), Some("\u{001f}1\u{0000}\u{0000}"));
    }
}
