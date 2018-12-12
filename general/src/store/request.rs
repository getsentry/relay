use lazy_static::lazy_static;
use regex::Regex;
use serde::de::IgnoredAny;
use url::Url;

use crate::protocol::{Query, Request};
use crate::types::{Annotated, ErrorKind, Meta, Object, Value, ValueAction, FromValue};

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
    let url_result = match request.url.value() {
        Some(url_string) => Url::parse(url_string),
        None => return,
    };

    let mut url = match url_result {
        Ok(url) => url,
        Err(_) => {
            // TODO: Remove value here or not?
            request.url.meta_mut().add_error(ErrorKind::InvalidData);
            return;
        }
    };

    // If either the query string or fragment is specified both as part of
    // the URL and as separate attribute, the attribute wins.
    if request.query_string.value().is_none() {
        request.query_string.set_value(Some(Query(
            url.query_pairs()
                .map(|(k, v)| (k.into(), Annotated::new(v.into())))
                .collect(),
        )));
    }

    if request.fragment.value().is_none() {
        request
            .fragment
            .set_value(url.fragment().map(str::to_string));
    }

    // Remove the fragment and query since they have been moved to their own
    // parameters to avoid duplication or inconsistencies.
    url.set_fragment(None);
    url.set_query(None);

    // TODO: Check if this generates unwanted effects with `meta.remarks`
    // when the URL was already PII stripped.
    request.url.set_value(Some(url.into_string()));
}

fn normalize_method(method: &mut String, meta: &mut Meta) -> ValueAction {
    method.make_ascii_uppercase();

    if !meta.has_errors() && METHOD_RE.is_match(&method) {
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
            if let Some((
                Annotated(Some(ref k), _),
                Annotated(Some(ref v), _)
            )) = item.value() {
                if k != "Cookie" {
                    return true;
                }

                let new_cookies = FromValue::from_value(
                    Annotated::new(Value::String(v.clone()))
                );

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
fn test_cookies_in_header() {
    use crate::protocol::{Headers, Cookies};

    let mut request = Request {
        url: Annotated::new("http://example.com".to_string()),
        headers: Annotated::new(Headers(vec![
            Annotated::new((Annotated::new("Cookie".to_string()), Annotated::new("a=b;c=d".to_string())))
        ].into())),
        ..Default::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request.cookies,
        Annotated::new(Cookies({
            let mut map = Object::new();
            map.insert("a".to_string(), Annotated::new("b".to_string()));
            map.insert("c".to_string(), Annotated::new("d".to_string()));
            map
        }))
    );
}


#[test]
fn test_cookies_in_header_not_overridden() {
    use crate::protocol::{Headers, Cookies};

    let mut request = Request {
        url: Annotated::new("http://example.com".to_string()),
        headers: Annotated::new(Headers(vec![
            Annotated::new((Annotated::new("Cookie".to_string()), Annotated::new("a=b;c=d".to_string())))
        ].into())),
        cookies: Annotated::new(Cookies({
            let mut map = Object::new();
            map.insert("foo".to_string(), Annotated::new("bar".to_string()));
            map
        })),
        ..Default::default()
    };

    normalize_request(&mut request, None);

    assert_eq_dbg!(
        request.cookies,
        Annotated::new(Cookies({
            let mut map = Object::new();
            map.insert("foo".to_string(), Annotated::new("bar".to_string()));
            map
        }))
    );
}
