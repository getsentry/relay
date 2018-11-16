use lazy_static::lazy_static;
use regex::Regex;
use serde::de::IgnoredAny;
use url::Url;

use crate::protocol::{Query, Request};
use crate::types::{Annotated, Object, Value};

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

fn normalize_url(mut request: Request) -> Request {
    if let Annotated(Some(url_string), mut meta) = request.url {
        match Url::parse(&url_string) {
            Ok(mut url) => {
                // If either the query string or fragment is specified both as part of
                // the URL and as separate attribute, the attribute wins.
                request.query_string = request.query_string.or_else(|| {
                    Query(
                        url.query_pairs()
                            .map(|(k, v)| (k.into(), Annotated::new(v.into())))
                            .collect(),
                    )
                });

                request.fragment = request
                    .fragment
                    .or_else(|| url.fragment().map(str::to_string));

                // Remove the fragment and query since they have been moved to their own
                // parameters to avoid duplication or inconsistencies.
                url.set_fragment(None);
                url.set_query(None);

                // TODO: Check if this generates unwanted effects with `meta.remarks`
                // when the URL was already PII stripped.
                request.url = Annotated(Some(url.into_string()), meta);
            }
            Err(err) => {
                meta.add_error(err.to_string(), None);
                request.url = Annotated(Some(url_string), meta);
            }
        }
    }

    request
}

fn normalize_method(method: Annotated<String>) -> Annotated<String> {
    method
        .filter_map(Annotated::is_valid, |method| method.to_uppercase())
        .filter_map(Annotated::is_valid, |method| {
            if METHOD_RE.is_match(&method) {
                Ok(method)
            } else {
                Err(Annotated::from_error("invalid http method", None))
            }
        })
}

fn set_auto_remote_addr(
    env: Annotated<Object<Value>>,
    remote_addr: &str,
) -> Annotated<Object<Value>> {
    env.and_then(|mut env| {
        env.entry("REMOTE_ADDR".to_string()).and_modify(|value| {
            value.modify(|value| {
                value.filter_map(Annotated::is_valid, |v| match v.as_str() {
                    Some("{{auto}}") => Value::String(remote_addr.to_string()),
                    _ => v,
                })
            })
        });
        env
    })
}

pub fn normalize_request(mut request: Request, client_ip: Option<&str>) -> Request {
    request.method = normalize_method(request.method);

    if request.url.is_valid() {
        request = normalize_url(request);
    }

    if let Some(ref client_ip) = client_ip {
        request.env = set_auto_remote_addr(request.env, client_ip);
    }

    if !request.inferred_content_type.is_present() {
        let content_type = request
            .headers
            .value()
            .and_then(|headers| headers.get("Content-Type"))
            .and_then(|annotated| annotated.value().cloned())
            .or_else(|| infer_content_type(&request.data));

        request.inferred_content_type.set_value(content_type);
    }

    request
}
