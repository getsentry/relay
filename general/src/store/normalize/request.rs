use lazy_static::lazy_static;
use regex::Regex;
use url::Url;

use crate::protocol::{PairList, Query, Request};
use crate::types::{Annotated, ErrorKind, FromValue, Meta, Object, Value, ValueAction};

const ELLIPSIS: char = '\u{2026}';

lazy_static! {
    static ref METHOD_RE: Regex = Regex::new(r"^[A-Z\-_]{3,32}$").unwrap();
}

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

    let url = match Url::parse(url_string) {
        Ok(url) => url,
        Err(_) => {
            request.url.meta_mut().add_error(ErrorKind::InvalidData);
            return;
        }
    };

    // Separate the query string and fragment bits into dedicated fields. If
    // both the URL and the fields have been set, the fields take precedence.
    if request.query_string.value().is_none() {
        let pairs: Vec<_> = url
            .query_pairs()
            .map(|(k, v)| {
                Annotated::new((
                    Annotated::new(k.to_string()),
                    Annotated::new(v.to_string().into()),
                ))
            })
            .collect();

        if !pairs.is_empty() {
            request.query_string.set_value(Some(Query(PairList(pairs))));
        }
    }

    if request.fragment.value().is_none() {
        request
            .fragment
            .set_value(url.fragment().map(str::to_string));
    }

    // Remove the fragment and query string from the URL to avoid duplication
    // or inconsistencies. We do not use `url.to_string()` here to prevent
    // urlencoding of special characters in the path segment of the URL or
    // normalization of partial URLs. That is expected to occur after PII
    // stripping.
    let url_end_index = match (url_string.find('?'), url_string.find('#')) {
        (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
        (a, b) => a.or(b),
    };

    if let Some(index) = url_end_index {
        url_string.truncate(index);
    }
}

fn normalize_method(method: &mut String, meta: &mut Meta) -> ValueAction {
    method.make_ascii_uppercase();

    if !meta.has_errors() && !METHOD_RE.is_match(&method) {
        meta.add_error(ErrorKind::InvalidData);
        return ValueAction::DeleteSoft;
    }

    ValueAction::Keep
}

fn set_auto_remote_addr(env: &mut Object<Value>, remote_addr: &str) {
    if let Some(entry) = env.get_mut("REMOTE_ADDR") {
        if let Some(value) = entry.value_mut() {
            if value.as_str() == Some("{{auto}}") {
                *value = Value::String(remote_addr.to_string());
            }
        }
    }
}

/// Checks for the likelyhood of a parsed urlencoded body.
fn is_valid_urlencoded(object: &Object<Value>) -> bool {
    // `serde_urlencoded` can decode any string with valid characters into an object. However, we
    // need to account for false-positives in the following cases:
    //  - A string "foo" is decoded as {"foo": ""} (check for single empty value)
    //  - A base64 encoded string "dGU=" also decodes with a single empty value
    //  - A base64 encoded string "dA==" decodes as {"dA": "="} (check for single =)
    //  - Any other string containing equals or ampersand will decode properly

    if object.len() > 1 {
        return true;
    }

    match object.values().next().and_then(|a| a.as_str()) {
        Some(s) => s != "" && s != "=",
        None => false,
    }
}

fn parse_raw_data(request: &Request) -> Option<(&'static str, Value)> {
    let raw = request.data.as_str()?;

    // TODO: Try to decode base64 first

    if let Ok(value) = serde_json::from_str(raw) {
        return Some(("application/json", value));
    }

    if let Ok(Value::Object(value)) = serde_urlencoded::from_str(raw) {
        if is_valid_urlencoded(&value) {
            return Some(("application/x-www-form-urlencoded", Value::Object(value)));
        }
    }

    None
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
        request.inferred_content_type = Annotated::from(content_type.to_string());
    } else {
        request.inferred_content_type = request
            .headers
            .value()
            .and_then(|headers| headers.get_header("Content-Type"))
            .map(|value| value.to_string())
            .into();
    }
}

pub fn normalize_request(request: &mut Request, client_ip: Option<&str>) {
    request.method.apply(normalize_method);
    normalize_url(request);
    normalize_data(request);

    if let Some(ref client_ip) = client_ip {
        request
            .env
            .apply(|env, _meta| set_auto_remote_addr(env, client_ip));
    }

    if let (None, Some(ref mut headers)) = (request.cookies.value(), request.headers.value_mut()) {
        let cookies = &mut request.cookies;
        headers.retain(|item| {
            if let Some((Annotated(Some(ref k), _), Annotated(Some(ref v), _))) = item.value() {
                if k != "Cookie" {
                    return true;
                }

                let new_cookies = FromValue::from_value(Annotated::new(Value::String(v.clone())));

                if new_cookies.meta().has_errors() {
                    return true;
                }

                *cookies = new_cookies;
                false
            } else {
                true
            }
        });
    }
}

#[test]
fn test_url_truncation() {
    let mut request = Request {
        url: Annotated::new("http://example.com/path?foo#bar".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path"));
}

#[test]
fn test_url_truncation_reversed() {
    let mut request = Request {
        // The query string is empty and the fragment is "foo?bar" here
        url: Annotated::new("http://example.com/path#foo?bar".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path"));
}

#[test]
fn test_url_with_ellipsis() {
    let mut request = Request {
        url: Annotated::new("http://example.com/path…".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path..."));
}

#[test]
fn test_url_with_qs_and_fragment() {
    use crate::protocol::Query;

    let mut request = Request {
        url: Annotated::new("http://example.com/path?some=thing#else".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request,
        Request {
            url: Annotated::new("http://example.com/path".to_string()),
            query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                Annotated::new("some".to_string()),
                Annotated::new("thing".to_string().into()),
            )),]))),
            fragment: Annotated::new("else".to_string()),
            ..Request::default()
        }
    );
}

#[test]
fn test_url_precedence() {
    use crate::protocol::Query;

    let mut request = Request {
        url: Annotated::new("http://example.com/path?completely=different#stuff".to_string()),
        query_string: Annotated::new(Query(PairList(vec![Annotated::new((
            Annotated::new("some".to_string()),
            Annotated::new("thing".to_string().into()),
        ))]))),
        fragment: Annotated::new("else".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request,
        Request {
            url: Annotated::new("http://example.com/path".to_string()),
            query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                Annotated::new("some".to_string()),
                Annotated::new("thing".to_string().into()),
            )),]))),
            fragment: Annotated::new("else".to_string()),
            ..Request::default()
        }
    );
}

#[test]
fn test_query_string_empty_value() {
    use crate::protocol::Query;

    let mut request = Request {
        url: Annotated::new("http://example.com/path?some".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request,
        Request {
            url: Annotated::new("http://example.com/path".to_string()),
            query_string: Annotated::new(Query(PairList(vec![Annotated::new((
                Annotated::new("some".to_string()),
                Annotated::new("".to_string().into()),
            )),]))),
            ..Request::default()
        }
    );
}

#[test]
fn test_cookies_in_header() {
    use crate::protocol::{Cookies, Headers};

    let mut request = Request {
        url: Annotated::new("http://example.com".to_string()),
        headers: Annotated::new(Headers(PairList(vec![Annotated::new((
            Annotated::new("Cookie".to_string()),
            Annotated::new("a=b;c=d".to_string()),
        ))]))),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request.cookies,
        Annotated::new(Cookies(PairList(vec![
            Annotated::new((
                Annotated::new("a".to_string()),
                Annotated::new("b".to_string()),
            )),
            Annotated::new((
                Annotated::new("c".to_string()),
                Annotated::new("d".to_string()),
            )),
        ])))
    );
}

#[test]
fn test_cookies_in_header_not_overridden() {
    use crate::protocol::{Cookies, Headers};

    let mut request = Request {
        url: Annotated::new("http://example.com".to_string()),
        headers: Annotated::new(Headers(
            vec![Annotated::new((
                Annotated::new("Cookie".to_string()),
                Annotated::new("a=b;c=d".to_string()),
            ))]
            .into(),
        )),
        cookies: Annotated::new(Cookies(PairList(vec![Annotated::new((
            Annotated::new("foo".to_string()),
            Annotated::new("bar".to_string()),
        ))]))),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request.cookies,
        Annotated::new(Cookies(PairList(vec![Annotated::new((
            Annotated::new("foo".to_string()),
            Annotated::new("bar".to_string()),
        ))])))
    );
}

#[test]
fn test_method_invalid() {
    let mut request = Request {
        method: Annotated::new("!!!!".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(request.method.value(), None);
}

#[test]
fn test_method_valid() {
    let mut request = Request {
        method: Annotated::new("POST".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(request.method.as_str(), Some("POST"));
}

#[test]
fn test_infer_json() {
    let mut request = Request {
        data: Annotated::from(Value::String(r#"{"foo":"bar"}"#.to_string())),
        ..Request::default()
    };

    let mut expected_value = Object::new();
    expected_value.insert(
        "foo".to_string(),
        Annotated::from(Value::String("bar".into())),
    );

    normalize_request(&mut request, None);
    assert_eq_dbg!(
        request.inferred_content_type.as_str(),
        Some("application/json")
    );
    assert_eq_dbg!(request.data.value(), Some(&Value::Object(expected_value)));
}

#[test]
fn test_broken_json_with_fallback() {
    use crate::protocol::Headers;

    let mut request = Request {
        data: Annotated::from(Value::String(r#"{"foo":"b"#.to_string())),
        headers: Annotated::from(Headers(PairList(vec![Annotated::new((
            Annotated::new("Content-Type".to_string()),
            Annotated::new("text/plain".to_string()),
        ))]))),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.inferred_content_type.as_str(), Some("text/plain"));
    assert_eq_dbg!(request.data.as_str(), Some(r#"{"foo":"b"#));
}

#[test]
fn test_broken_json_without_fallback() {
    let mut request = Request {
        data: Annotated::from(Value::String(r#"{"foo":"b"#.to_string())),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some(r#"{"foo":"b"#));
}

#[test]
fn test_infer_url_encoded() {
    let mut request = Request {
        data: Annotated::from(Value::String(r#"foo=bar"#.to_string())),
        ..Request::default()
    };

    let mut expected_value = Object::new();
    expected_value.insert(
        "foo".to_string(),
        Annotated::from(Value::String("bar".into())),
    );

    normalize_request(&mut request, None);
    assert_eq_dbg!(
        request.inferred_content_type.as_str(),
        Some("application/x-www-form-urlencoded")
    );
    assert_eq_dbg!(request.data.value(), Some(&Value::Object(expected_value)));
}

#[test]
fn test_infer_url_false_positive() {
    let mut request = Request {
        data: Annotated::from(Value::String("dGU=".to_string())),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some("dGU="));
}

#[test]
fn test_infer_url_encoded_base64() {
    let mut request = Request {
        data: Annotated::from(Value::String("dA==".to_string())),
        ..Request::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some("dA=="));
}
