use lazy_static::lazy_static;
use regex::Regex;
use serde::de::IgnoredAny;
use url::Url;

use crate::protocol::{PairList, Query, Request};
use crate::types::{Annotated, ErrorKind, FromValue, Meta, Object, Value, ValueAction};

const ELLIPSIS: char = '\u{2026}';

lazy_static! {
    static ref METHOD_RE: Regex = Regex::new(r"^[A-Z\-_]{3,32}$").unwrap();
}

fn infer_content_type(body: &Annotated<Value>) -> Option<String> {
    let body = body.value()?.as_str()?;

    if serde_json::from_str::<IgnoredAny>(body).is_ok() {
        Some("application/json".to_string())
    } else if serde_urlencoded::from_str::<IgnoredAny>(body).is_ok() {
        Some("application/x-www-form-urlencoded".to_string())
    } else {
        None
    }
}

fn normalize_url(request: &mut Request) {
    let url_string = match request.url.value_mut() {
        Some(url_string) => url_string,
        None => return,
    };

    // Special case: JavaScript used to truncate with a unicode ellipsis. Since
    // that would be encoded in the URL, we trim it first and append it again at
    // the end.
    let ellipsis = if url_string.ends_with(ELLIPSIS) {
        url_string.truncate(url_string.len() - ELLIPSIS.len_utf8());
        true
    } else {
        false
    };

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

    if ellipsis {
        url_string.push(ELLIPSIS);
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

pub fn normalize_request(request: &mut Request, client_ip: Option<&str>) {
    request.method.apply(normalize_method);
    normalize_url(request);

    if let Some(ref client_ip) = client_ip {
        request
            .env
            .apply(|env, _meta| set_auto_remote_addr(env, client_ip));
    }

    if request.inferred_content_type.value().is_none() {
        let content_type = request
            .headers
            .value()
            .and_then(|headers| headers.get_header("Content-Type"))
            .map(|value| value.to_string())
            .or_else(|| infer_content_type(&request.data));

        request.inferred_content_type.set_value(content_type);
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
        ..Default::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path"));
}

#[test]
fn test_url_truncation_reversed() {
    let mut request = Request {
        // The query string is empty and the fragment is "foo?bar" here
        url: Annotated::new("http://example.com/path#foo?bar".to_string()),
        ..Default::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path"));
}

#[test]
fn test_url_with_ellipsis() {
    let mut request = Request {
        url: Annotated::new("http://example.com/path…".to_string()),
        ..Default::default()
    };

    normalize_request(&mut request, None);
    assert_eq_dbg!(request.url.as_str(), Some("http://example.com/path…"));
}

#[test]
fn test_url_with_qs_and_fragment() {
    use crate::protocol::Query;

    let mut request = Request {
        url: Annotated::new("http://example.com/path?some=thing#else".to_string()),
        ..Default::default()
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
            ..Default::default()
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
        ..Default::default()
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
            ..Default::default()
        }
    );
}

#[test]
fn test_query_string_empty_value() {
    use crate::protocol::Query;

    let mut request = Request {
        url: Annotated::new("http://example.com/path?some".to_string()),
        ..Default::default()
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
            ..Default::default()
        }
    );
}

#[test]
fn test_cookies_in_header() {
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(request.method.0, None);
}

#[test]
fn test_method_valid() {
    let mut request = Request {
        method: Annotated::new("POST".to_string()),
        ..Default::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(request.method.0, Some("POST".to_string()));
}
