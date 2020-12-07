use lazy_static::lazy_static;
use regex::Regex;
use url::Url;

use crate::protocol::{Query, Request};
use crate::types::{Annotated, ErrorKind, Meta, ProcessingAction, ProcessingResult, Value};

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
                *url_string = url.into_string();
            }
        }
        Err(_) => {
            // The URL is invalid, but we can still apply heuristics to parse the query
            // string and put the fragment in its own field.
            if let Some(fragment_index) = url_string.find('#') {
                let fragment = &url_string[fragment_index + 1..];
                if !fragment.is_empty() && request.fragment.value().is_none() {
                    request.fragment.set_value(Some(fragment.to_string()));
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

fn normalize_method(method: &mut String, meta: &mut Meta) -> ProcessingResult {
    method.make_ascii_uppercase();

    if !meta.has_errors() && !METHOD_RE.is_match(&method) {
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
            .map_or(false, |s| !matches!(s, "" | "="));

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
    } else if let Some(value) = urlencoded_from_str(raw) {
        Some(("application/x-www-form-urlencoded", value))
    } else {
        None
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
        request.inferred_content_type = Annotated::from(content_type.to_string());
    } else {
        request.inferred_content_type = request
            .headers
            .value()
            .and_then(|headers| headers.get_header("Content-Type"))
            .map(|value| value.split(';').next().unwrap_or(&value).to_string())
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

    if let Ok(new_cookies) = crate::protocol::Cookies::parse(cookie_header) {
        request.cookies = Annotated::from(new_cookies);
        headers.remove("Cookie");
    }
}

pub fn normalize_request(request: &mut Request) -> ProcessingResult {
    request.method.apply(normalize_method)?;
    normalize_url(request);
    normalize_data(request);
    normalize_cookies(request);
    Ok(())
}

#[cfg(test)]
use crate::protocol::PairList;

#[cfg(test)]
use crate::types::Object;

#[test]
fn test_url_truncation() {
    let mut request = Request {
        url: Annotated::new("http://example.com/path?foo#bar".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path"));
}

#[test]
fn test_url_truncation_reversed() {
    let mut request = Request {
        // The query string is empty and the fragment is "foo?bar" here
        url: Annotated::new("http://example.com/path#foo?bar".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path"));
}

#[test]
fn test_url_with_ellipsis() {
    let mut request = Request {
        url: Annotated::new("http://example.com/path…".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path..."));
}

#[test]
fn test_url_with_qs_and_fragment() {
    use crate::protocol::Query;

    let mut request = Request {
        url: Annotated::new("http://example.com/path?some=thing#else".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();

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
fn test_url_only_path() {
    let mut request = Request {
        url: Annotated::from("metamask/popup.html#".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(
        request,
        Request {
            url: Annotated::new("metamask/popup.html".to_string()),
            ..Request::default()
        }
    );
}

#[test]
fn test_url_punycoded() {
    let mut request = Request {
        url: Annotated::new("http://göögle.com/".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();

    assert_eq_dbg!(
        request,
        Request {
            url: Annotated::new("http://xn--ggle-5qaa.com/".to_string()),
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

    normalize_request(&mut request).unwrap();

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

    normalize_request(&mut request).unwrap();

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
            Annotated::new("Cookie".to_string().into()),
            Annotated::new("a=b;c=d".to_string().into()),
        ))]))),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();

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

    assert_eq_dbg!(request.headers.value().unwrap().get_header("Cookie"), None);
}

#[test]
fn test_cookies_in_header_dont_override_cookies() {
    use crate::protocol::{Cookies, Headers};

    let mut request = Request {
        url: Annotated::new("http://example.com".to_string()),
        headers: Annotated::new(Headers(
            vec![Annotated::new((
                Annotated::new("Cookie".to_string().into()),
                Annotated::new("a=b;c=d".to_string().into()),
            ))]
            .into(),
        )),
        cookies: Annotated::new(Cookies(PairList(vec![Annotated::new((
            Annotated::new("foo".to_string()),
            Annotated::new("bar".to_string()),
        ))]))),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();

    assert_eq_dbg!(
        request.cookies,
        Annotated::new(Cookies(PairList(vec![Annotated::new((
            Annotated::new("foo".to_string()),
            Annotated::new("bar".to_string()),
        ))])))
    );

    // Cookie header is removed when explicit cookies are given
    assert_eq_dbg!(request.headers.value().unwrap().get_header("Cookie"), None);
}

#[test]
fn test_method_invalid() {
    let mut request = Request {
        method: Annotated::new("!!!!".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();

    assert_eq_dbg!(request.method.value(), None);
}

#[test]
fn test_method_valid() {
    let mut request = Request {
        method: Annotated::new("POST".to_string()),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();

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

    normalize_request(&mut request).unwrap();
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
            Annotated::new("Content-Type".to_string().into()),
            Annotated::new("text/plain; encoding=utf-8".to_string().into()),
        ))]))),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.inferred_content_type.as_str(), Some("text/plain"));
    assert_eq_dbg!(request.data.as_str(), Some(r#"{"foo":"b"#));
}

#[test]
fn test_broken_json_without_fallback() {
    let mut request = Request {
        data: Annotated::from(Value::String(r#"{"foo":"b"#.to_string())),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
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

    normalize_request(&mut request).unwrap();
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

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some("dGU="));
}

#[test]
fn test_infer_url_encoded_base64() {
    let mut request = Request {
        data: Annotated::from(Value::String("dA==".to_string())),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some("dA=="));
}

#[test]
fn test_infer_xml() {
    let mut request = Request {
        data: Annotated::from(Value::String("<?xml version=\"1.0\" ?>".to_string())),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some("<?xml version=\"1.0\" ?>"));
}

#[test]
fn test_infer_binary() {
    let mut request = Request {
        data: Annotated::from(Value::String("\u{001f}1\u{0000}\u{0000}".to_string())),
        ..Request::default()
    };

    normalize_request(&mut request).unwrap();
    assert_eq_dbg!(request.inferred_content_type.value(), None);
    assert_eq_dbg!(request.data.as_str(), Some("\u{001f}1\u{0000}\u{0000}"));
}
